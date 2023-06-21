/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A generic jobState for jobs.
 */
public enum JobState implements Serializable {
    UNSTARTED, // Initial state before submission.
    HOLD,
    QUEUED,
    /**
     * It is used only in Roddy.
     */
    STARTED,
    /**
     * BEJob is still running; according to cluster monitor
     */
    RUNNING,
    SUSPENDED,
    /**
     * BEJob has failed
     */
    FAILED,
    /**
     * A job where we don't know, whether it is ok or failed.
     * Has to be read out from a jobstate logfile or retrieved from the job system.
     * E.g. PBS only shows C
     */
    COMPLETED_UNKNOWN,
    /**
     * A job which was successfully completed
     */
    COMPLETED_SUCCESSFUL,
    ABORTED,
    /**
     * BEJob jobState is unknown
     */
    UNKNOWN,
    /**
     * It is used only in Roddy.
     * Dummy jobs were not executed but can contain runtime information for future runs.
     */
    DUMMY;

    @Deprecated
    public boolean isPlannedOrRunning() {
        return this.isRunning() || this.isPlanned();
    }

    public boolean isPlanned() {
        return Arrays.asList(
                JobState.QUEUED,
                JobState.UNSTARTED,
                JobState.HOLD,
                JobState.SUSPENDED).contains(this);
    }

    @Deprecated
    public boolean isHeldOrSuspended() {
        return Arrays.asList(JobState.HOLD, JobState.SUSPENDED).contains(this);
    }

    public boolean isDummy() {
        return this == DUMMY;
    }

    public boolean isStarted() {
        return this == STARTED;
    }

    public boolean isRunning() {
        return this == JobState.RUNNING;
    }

    @Deprecated
    public boolean isRunningOrStarted() {
        return Arrays.asList(JobState.RUNNING, JobState.STARTED).contains(this);
    }

    public boolean isUnknown() {
        return this == UNKNOWN;
    }

    public boolean isSuccessful() {
        return this == COMPLETED_SUCCESSFUL;
    }

    public boolean isFailed() { return Arrays.asList(JobState.FAILED, JobState.ABORTED).contains(this); }

    public boolean isCompleted() { return Arrays.asList(JobState.COMPLETED_SUCCESSFUL, JobState.COMPLETED_UNKNOWN).contains(this); }
}
