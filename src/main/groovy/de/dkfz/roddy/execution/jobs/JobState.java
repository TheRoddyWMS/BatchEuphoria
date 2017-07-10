/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs;

import java.io.Serializable;

/**
 * A generic jobState for jobs.
 */
public enum JobState implements Serializable {
    HOLD,
    QUEUED,
    /**
     * BEJob is still running
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
     * Dummy jobs were not executed but can contain runtime information for future runs.
     */
    DUMMY;

    public boolean isPlannedOrRunning() {
        return _isPlannedOrRunning(this);
    }

    public static boolean _isPlannedOrRunning(JobState jobState) {
        return jobState == JobState.RUNNING || jobState == JobState.QUEUED || jobState == JobState.HOLD;
    }

    public boolean isDummy() {
        return this == DUMMY;
    }

    public boolean isRunning() {
        return this == JobState.RUNNING;
    }

    public boolean isUnknown() { return  this == UNKNOWN;}


}
