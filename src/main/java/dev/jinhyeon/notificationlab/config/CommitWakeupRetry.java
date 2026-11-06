package dev.jinhyeon.notificationlab.config;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Map;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.WakeupException;

/** Retry only Spring's explicit completed-offset commit, once, within its original time budget. */
final class CommitWakeupRetry {
    private CommitWakeupRetry() { }

    static <K, V> Consumer<K, V> wrap(Consumer<K, V> delegate, Runnable onRetry) {
        return wrap(delegate, onRetry, System::nanoTime);
    }

    @SuppressWarnings("unchecked")
    static <K, V> Consumer<K, V> wrap(Consumer<K, V> delegate, Runnable onRetry, LongSupplier clock) {
        return (Consumer<K, V>) Proxy.newProxyInstance(Consumer.class.getClassLoader(), new Class<?>[]{Consumer.class},
                (proxy, method, arguments) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "equals" -> proxy == arguments[0];
                            case "hashCode" -> System.identityHashCode(proxy);
                            default -> invoke(delegate, method, arguments);
                        };
                    }
                    // Never retry polls, listener invocation, async commits or implicit-position commits.
                    if (!method.getName().equals("commitSync") || arguments == null || arguments.length != 2
                            || !(arguments[0] instanceof Map<?, ?> offsets) || !(arguments[1] instanceof Duration timeout)) {
                        return invoke(delegate, method, arguments);
                    }
                    var attempt = arguments.clone();
                    attempt[0] = Map.copyOf(offsets);
                    long started = clock.getAsLong();
                    try {
                        return invoke(delegate, method, attempt);
                    } catch (WakeupException wakeup) {
                        Duration remaining = timeout.minusNanos(Math.max(0, clock.getAsLong() - started));
                        if (remaining.isZero() || remaining.isNegative()) throw wakeup;
                        attempt[1] = remaining;
                        onRetry.run();
                        // A second failure propagates to the existing fail-closed container handler.
                        return invoke(delegate, method, attempt);
                    }
                });
    }

    private static Object invoke(Object delegate, Method method, Object[] arguments) throws Throwable {
        try { return method.invoke(delegate, arguments); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
}
