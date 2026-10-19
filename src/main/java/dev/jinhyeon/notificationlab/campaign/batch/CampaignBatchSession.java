package dev.jinhyeon.notificationlab.campaign.batch;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.MysqlDataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.database.support.DefaultDataFieldMaxValueIncrementerFactory;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

/** One synchronous run, one non-reconnecting physical session for ownership and all durable work. */
final class CampaignBatchSession implements AutoCloseable {
    static final String PROTOCOL = "mysql-session-v1";
    private final Connection connection;
    final JdbcTemplate jdbc;
    final DataSourceTransactionManager transactions;
    final JobRepository repository;
    final JobExplorer explorer;
    final TaskExecutorJobLauncher launcher;

    private CampaignBatchSession(Connection connection, DataSource sequences, UUID campaignId) throws Exception {
        this.connection = connection;
        // A failover proxy or transparent reconnect could write after losing GET_LOCK ownership.
        if (!(connection instanceof ConnectionImpl mysql)
                || mysql.getPropertySet().getBooleanProperty(PropertyKey.autoReconnect).getValue()
                || mysql.getPropertySet().getBooleanProperty(PropertyKey.autoReconnectForPools).getValue()) {
            throw new IllegalArgumentException("Campaign Batch requires a single-host MySQL connection without reconnect");
        }
        var source = new SingleConnectionDataSource(connection, true);
        jdbc = new JdbcTemplate(source);
        String database = jdbc.queryForObject("SELECT DATABASE()", String.class);
        Integer acquired = jdbc.queryForObject("SELECT GET_LOCK(?, 0)", Integer.class, lockName(database, campaignId));
        if (!Integer.valueOf(1).equals(acquired)) {
            throw new JobExecutionAlreadyRunningException("Campaign MySQL session is still owned");
        }
        transactions = new DataSourceTransactionManager(source);
        var factory = new JobRepositoryFactoryBean();
        factory.setDataSource(source);
        factory.setTransactionManager(transactions);
        factory.setDatabaseType("MYSQL");
        // MySQL's incrementer commits its own transaction. Never let it commit the run's connection.
        // Only sequence numbers use the pool; no business rows or execution metadata use it.
        factory.setIncrementerFactory(new DefaultDataFieldMaxValueIncrementerFactory(sequences));
        factory.afterPropertiesSet();
        repository = factory.getObject();
        var exploration = new JobExplorerFactoryBean();
        exploration.setDataSource(source);
        exploration.setTransactionManager(transactions);
        exploration.afterPropertiesSet();
        explorer = exploration.getObject();
        launcher = new TaskExecutorJobLauncher();
        launcher.setJobRepository(repository);
        launcher.afterPropertiesSet(); // Default synchronous executor: the connection must never be shared across threads.
    }

    static CampaignBatchSession open(DataSourceProperties properties, DataSource sequences, UUID id) throws Exception {
        // Deliberately unpooled: close must terminate the owner session, not return its named lock to Hikari.
        var source = new MysqlDataSource();
        source.setUrl(properties.determineUrl());
        source.setUser(properties.determineUsername());
        source.setPassword(properties.determinePassword());
        Connection connection = source.getConnection();
        try {
            return new CampaignBatchSession(connection, sequences, id);
        } catch (Exception failure) {
            try { connection.close(); } catch (Exception ignored) { /* Preserve the original failure. */ }
            throw failure;
        }
    }

    static String lockName(String database, UUID id) {
        return "notification-lab:batch:" + UUID.nameUUIDFromBytes((database + ":" + id).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws java.sql.SQLException {
        // Session termination rolls back uncommitted work and releases GET_LOCK, including after failures.
        connection.close();
    }
}
