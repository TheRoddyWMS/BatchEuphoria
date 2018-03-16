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

    public boolean isPlannedOrRunning() {
        return _isPlannedOrRunning(this);
    }

    public static boolean _isPlannedOrRunning(JobState jobState) {
        return Arrays.asList(JobState.RUNNING, JobState.QUEUED, JobState.HOLD, JobState.STARTED, JobState.UNSTARTED).contains(jobState);
    }

    public boolean isDummy() {
        return this == DUMMY;
    }

    public boolean isRunning() {
        return this == JobState.RUNNING;
    }

    public boolean isUnknown() { return  this == UNKNOWN;}

    public boolean isSuccessful() { return this == COMPLETED_SUCCESSFUL; }

}
