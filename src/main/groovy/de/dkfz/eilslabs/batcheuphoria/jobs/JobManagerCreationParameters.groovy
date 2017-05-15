/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs

import groovy.transform.CompileStatic

import static de.dkfz.eilslabs.batcheuphoria.jobs.JobManager.JOBMANAGER_DEFAULT_CREATE_DAEMON
import static de.dkfz.eilslabs.batcheuphoria.jobs.JobManager.JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY
import static de.dkfz.eilslabs.batcheuphoria.jobs.JobManager.JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY
import static de.dkfz.eilslabs.batcheuphoria.jobs.JobManager.JOBMANAGER_DEFAULT_UPDATEINTERVAL;

/**
 * Created by heinold on 28.02.17.
 */
@CompileStatic
class JobManagerCreationParameters {

    boolean createDaemon = JOBMANAGER_DEFAULT_CREATE_DAEMON
    int updateInterval = JOBMANAGER_DEFAULT_UPDATEINTERVAL
    String userIdForJobQueries = ""
    boolean trackUserJobsOnly = JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY
    boolean trackOnlyStartedJobs = JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY
    String jobIDIdentifier = JobManager.BE_DEFAULT_JOBID
    String jobArrayIDIdentifier = JobManager.BE_DEFAULT_JOBARRAYINDEX
    String jobScratchIdentifier = JobManager.BE_DEFAULT_JOBSCRATCH
    String userGroup
    String userAccount
    String userEmail
    String userMask

    JobManagerCreationParameters() {
    }

    JobManagerCreationParameters(boolean createDaemon, int updateInterval, String userIdForJobQueries, boolean trackUserJobsOnly, boolean trackOnlyStartedJobs, String jobIDIdentifier, String jobArrayIDIdentifier, String jobScratchIdentifier, String userAccount, String userEmail, String userMask, String userGroup) {
        this.createDaemon = createDaemon
        this.updateInterval = updateInterval
        this.userIdForJobQueries = userIdForJobQueries
        this.trackUserJobsOnly = trackUserJobsOnly
        this.trackOnlyStartedJobs = trackOnlyStartedJobs
        this.jobIDIdentifier = jobIDIdentifier
        this.jobArrayIDIdentifier = jobArrayIDIdentifier
        this.jobScratchIdentifier = jobScratchIdentifier
        this.userMask = userMask
        this.userGroup = userGroup
        this.userEmail = userEmail
        this.userAccount = userAccount
    }
}
