/*
 * Copyright (c) 2019 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/BatchEuphoria/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode

import java.time.ZonedDateTime

/**
 * The class contains the bare minimum of information about a job gathered by
 * BatchEuphoriaJobManager.queryJobInfo()
 * Note that all fields except jobID can be undefined / null!
 *
 * There are three different settings for the content of this class:
 * - A job was not found by the manager, the state will be set to UNKNOWN, with no endTime
 * - A job was found but is still running, the state will be set accordingly, with no endTime
 * - A job was found and is finished, the state will be set accordingly with the endTime set
 *
 * This class is a value class. Normally, we'd use final fields for all values or some kind of
 * builder interface to create instances of it. But due to the sheer size of ExtendedJobInfo,
 * we will refrain from both. Please make sure, that you do net mess things up.
 */
@CompileStatic
@EqualsAndHashCode
class JobInfo {

    BEJobID jobID

    /**
     * The date-time the job was completed.
     * Can be null, e.g. if the batch system cannot track the job anymore.
     * */
    ZonedDateTime endTime

    JobState jobState = JobState.UNKNOWN

    JobInfo(BEJobID jobID) {
        this.jobID = jobID
    }
}
