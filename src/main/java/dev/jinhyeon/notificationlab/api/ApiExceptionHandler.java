package dev.jinhyeon.notificationlab.api;

import dev.jinhyeon.notificationlab.notification.NotificationNotFoundException;
import dev.jinhyeon.notificationlab.notification.InvalidIdempotencyKeyException;
import dev.jinhyeon.notificationlab.notification.IdempotencyConflictException;
import dev.jinhyeon.notificationlab.notification.StoreCapacityExceededException;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler({DataAccessException.class, TransactionException.class})
    public ProblemDetail storageUnavailable(Exception exception) {
        // SQL exceptions may contain bound recipient/payload values; do not log or echo them.
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "Notification storage is temporarily unavailable");
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class})
    public ProblemDetail invalidInput(Exception exception) {
        // Avoid echoing rejected recipient IDs/payloads or relying on default warning logs.
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Invalid notification request or ID");
    }

    @ExceptionHandler(InvalidIdempotencyKeyException.class)
    public ProblemDetail invalidIdempotencyKey(InvalidIdempotencyKeyException exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, exception.getMessage());
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    public ProblemDetail idempotencyConflict(IdempotencyConflictException exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, exception.getMessage());
    }

    @ExceptionHandler(NotificationNotFoundException.class)
    public ProblemDetail notFound(NotificationNotFoundException exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, exception.getMessage());
    }

    @ExceptionHandler(StoreCapacityExceededException.class)
    public ResponseEntity<ProblemDetail> full(StoreCapacityExceededException exception) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, exception.getMessage()));
    }
}
