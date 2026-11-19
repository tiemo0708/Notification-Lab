package dev.jinhyeon.notificationlab.worker;

import java.sql.SQLException;

/** Marks a connection failure before the claim can commit. */
final class WorkerDatabaseUnavailable extends RuntimeException {
    WorkerDatabaseUnavailable() {
        super("Worker database connection unavailable before claiming notification");
    }

    static boolean isConnectionFailure(RuntimeException failure) {
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < 8; depth++, cause = cause.getCause()) {
            if (cause instanceof SQLException sql && sql.getSQLState() != null && sql.getSQLState().startsWith("08")) {
                return true;
            }
        }
        return false;
    }
}
