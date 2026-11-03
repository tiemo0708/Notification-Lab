package dev.jinhyeon.notificationlab.campaign.batch;

/** Admission observation profiles; production delivery and offset settings remain independent. */
record ApiFailureObservationPlan(Profile profile, ApiFailurePilotSupport.Mode mode) {
    enum Profile { PILOT, LONG_RUN, REHEARSAL }

    void validate(boolean load, int targets, boolean fineTiming, boolean offsetComparison,
                  boolean scaling, boolean apiExtras) {
        if (profile == Profile.PILOT) {
            if (load || targets != 1000 || !fineTiming || !offsetComparison || !scaling || apiExtras) {
                throw new IllegalArgumentException("Failure pilot requires the dedicated 1k timing configuration");
            }
            return;
        }
        if (fineTiming || offsetComparison || scaling || apiExtras) {
            throw new IllegalArgumentException("Long-run observation preserves application delivery settings");
        }
        if (profile == Profile.LONG_RUN) {
            if (!load || targets != 1_000_000 || mode != ApiFailurePilotSupport.Mode.NONE) {
                throw new IllegalArgumentException("Long-run observation requires one million targets without fault injection");
            }
        } else if (load || targets != 1000) {
            throw new IllegalArgumentException("Long-run rehearsal is limited to exactly 1000 targets");
        }
    }
}
