package dev.jinhyeon.notificationlab.deadletter;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

public record RedriveRequest(@NotNull Reason reason, @AssertTrue boolean acknowledgePossibleDuplicate) {
    public enum Reason { PROVIDER_RECOVERED, CONFIGURATION_FIXED, MANUAL_REVIEW }
}
