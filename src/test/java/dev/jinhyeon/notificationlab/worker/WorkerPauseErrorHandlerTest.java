package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.config.WorkerProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerPauseErrorHandlerTest {
    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withUserConfiguration(WorkerPauseErrorHandler.class)
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
            .withBean(WorkerAdmission.class, () -> new WorkerAdmission(
                    new WorkerProperties(3, 3, 0, 1000), new SimpleMeterRegistry()));

    @Test
    void contextKeepsDefaultSixAndOnlyUsesTwelveWhenExplicitlyConfigured() {
        context.run(app -> {
            var handler = app.getBean(WorkerPauseErrorHandler.class);
            assertThat(handler.transientRecordLimit()).isEqualTo(6);
            assertThat(handler.transientRecordCount()).isZero();
            assertThat(app.getBean(MeterRegistry.class).get("notification.worker.quota.transient.capacity").gauge().value()).isEqualTo(6);
        });
        context.withPropertyValues("app.rate-limit.transient-recovery-max-records=12").run(app -> {
            var handler = app.getBean(WorkerPauseErrorHandler.class);
            assertThat(handler.transientRecordLimit()).isEqualTo(12);
            assertThat(handler.transientRecordCount()).isZero();
        });
    }

    @Test
    void invalidCapacityCannotStartTheHandler() {
        for (int capacity : new int[]{-1, 0, 13, Integer.MAX_VALUE}) {
            context.withPropertyValues("app.rate-limit.transient-recovery-max-records=" + capacity)
                    .run(app -> assertThat(app).hasFailed());
        }
    }
}
