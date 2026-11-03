package dev.jinhyeon.notificationlab.campaign.batch;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiFailureObservationPlanTest {
    @Test
    void longRunUsesExistingMillionBoundsWithoutFaultInjection() {
        var plan = new ApiFailureObservationPlan(ApiFailureObservationPlan.Profile.LONG_RUN, ApiFailurePilotSupport.Mode.NONE);
        var bounds = CampaignDeliveryBounds.forRun(true, 1_000_000);
        assertThatCode(() -> plan.validate(true, bounds.targets(), false, false, false, false)).doesNotThrowAnyException();
    }

    @Test
    void millionRunRejectsEverySyntheticFaultModeBeforeStartingInfrastructure() {
        for (var mode : ApiFailurePilotSupport.Mode.values()) {
            if (mode == ApiFailurePilotSupport.Mode.NONE) continue;
            var plan = new ApiFailureObservationPlan(ApiFailureObservationPlan.Profile.LONG_RUN, mode);
            assertThatThrownBy(() -> plan.validate(true, 1_000_000, false, false, false, false)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void longRunRejectsChangingDeliveryOrOtherObservationFactors() {
        var plan = new ApiFailureObservationPlan(ApiFailureObservationPlan.Profile.LONG_RUN, ApiFailurePilotSupport.Mode.NONE);
        for (int i = 0; i < 4; i++) {
            int enabled = i;
            assertThatThrownBy(() -> plan.validate(true, 1_000_000, enabled == 0, enabled == 1, enabled == 2, enabled == 3))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rehearsalAllowsControlsOnlyForExactlyOneThousandWithoutLoad() {
        for (var mode : ApiFailurePilotSupport.Mode.values()) {
            var plan = new ApiFailureObservationPlan(ApiFailureObservationPlan.Profile.REHEARSAL, mode);
            assertThatCode(() -> plan.validate(false, 1000, false, false, false, false)).doesNotThrowAnyException();
            for (int targets : new int[]{10_000, 100_000, 1_000_000}) {
                assertThatThrownBy(() -> plan.validate(false, targets, false, false, false, false)).isInstanceOf(IllegalArgumentException.class);
            }
            assertThatThrownBy(() -> plan.validate(true, 1000, false, false, false, false)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void existingPilotProfileKeepsItsSeparateTimingConfiguration() {
        var plan = new ApiFailureObservationPlan(ApiFailureObservationPlan.Profile.PILOT, ApiFailurePilotSupport.Mode.NONE);
        assertThatCode(() -> plan.validate(false, 1000, true, true, true, false)).doesNotThrowAnyException();
        assertThatThrownBy(() -> plan.validate(true, 1_000_000, true, true, true, false)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> plan.validate(false, 1000, false, false, false, false)).isInstanceOf(IllegalArgumentException.class);
    }
}
