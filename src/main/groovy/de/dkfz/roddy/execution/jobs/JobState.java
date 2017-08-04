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
    /**
     * BEJob is still running; according to cluster monitor
     */
    RUNNING,
    /**
     * BEJob has failed
     */
    FAILED,
    /**
     * BEJob was ok
     */
    OK,
    /**
     * A job where we don't know, whether it is ok or failed.
     * Has to be read out from a jobstate logfile or retrieved from the job system.
     * E.g. PBS only shows C
     */
    COMPLETED_UNKNOWN,
    /**
     * BEJob jobState is unknown
     */
    UNKNOWN,
    /**
     * BEJob jobState is unknown because it was freshly read out from a file.
     */
    UNKNOWN_READOUT,
    /**
     * Recently submitted job, jobState is unknown
     */
    UNKNOWN_SUBMITTED,
    /**
     * Jobs which were submitted but not started (i.e. due to crashed or cancelled succeeding jobs).
     */
    UNSTARTED,
    /**
     * Jobs which were started and which might be running.
     */
    STARTED,  // according to job state logfile
    HOLD,
    QUEUED,
    /**
     * Dummy jobs were not executed but can contain runtime information for future runs.
     */
    DUMMY, ABORTED, FAILED_POSSIBLE;

    public boolean isPlannedOrRunning() {
        return _isPlannedOrRunning(this);
    }

    public static boolean _isPlannedOrRunning(JobState jobState) {
        return jobState == JobState.UNSTARTED || jobState == JobState.RUNNING || jobState == JobState.QUEUED || jobState == JobState.HOLD;
    }

    public boolean isDummy() {
        return this == DUMMY;
    }

    public boolean isRunning() {
        return this == JobState.RUNNING;
    }

    public boolean isUnknown() { return  this == UNKNOWN || this == UNKNOWN_READOUT || this == UNKNOWN_SUBMITTED; }

    public static JobState parseJobState(String stateString) {
        JobState status = FAILED;
        if (stateString.equals("0") || stateString.equals("C"))    //Completed
            status = OK;
        else if (stateString.equals("E"))   //E - Exitting
            status = FAILED;
        else if (stateString.equals("A"))   //A - Aborted
            status = ABORTED;
        else if (stateString.equals("N"))   //N??
            status = FAILED;
        else if (stateString.equals("ABORTED"))   //Aborted due to failed parent job or due to a missing dependency.
            status = ABORTED;
        else if (stateString.equals("STARTED"))   //Started and possibly running
            status = STARTED;
        else
            status = FAILED;
        return status;
    }
}
