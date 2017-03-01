/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs

import static JobManager.*
import groovy.transform.CompileStatic

/**
 * Created by heinold on 28.02.17.
 */
@CompileStatic
class JobManagerCreationParametersBuilder {

    boolean createDaemon = JOBMANAGER_DEFAULT_CREATE_DAEMON
    int updateInterval = JOBMANAGER_DEFAULT_UPDATEINTERVAL
    String userIdForJobQueries = ""
    boolean trackUserJobsOnly = JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY
    boolean trackOnlyStartedJobs = JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY
    String jobIDIdentifier = JobManager.BE_DEFAULT_JOBID
    String jobArrayIDIdentifier = JobManager.BE_DEFAULT_JOBARRAYINDEX
    String jobScratchIdentifier = JobManager.BE_DEFAULT_JOBSCRATCH

    JobManagerCreationParametersBuilder setCreateDaemon(boolean createDaemon) {
        this.createDaemon = createDaemon
        return this;
    }

    JobManagerCreationParametersBuilder setUpdateInterval(int updateInterval) {
        this.updateInterval = updateInterval
        return this;
    }

    JobManagerCreationParametersBuilder setUserIdForJobQueries(String userIdForJobQueries) {
        this.userIdForJobQueries = userIdForJobQueries
        return this;
    }

    JobManagerCreationParametersBuilder setTrackUserJobsOnly(boolean trackUserJobsOnly) {
        this.trackUserJobsOnly = trackUserJobsOnly
        return this;
    }

    JobManagerCreationParametersBuilder setTrackOnlyStartedJobs(boolean trackOnlyStartedJobs) {
        this.trackOnlyStartedJobs = trackOnlyStartedJobs
        return this;
    }

    JobManagerCreationParametersBuilder setJobIDIdentifier(String jobIDIdentifier) {
        this.jobIDIdentifier = jobIDIdentifier
        return this;
    }

    JobManagerCreationParametersBuilder setJobArrayIDIdentifier(String jobArrayIDIdentifier) {
        this.jobArrayIDIdentifier = jobArrayIDIdentifier
        return this;
    }

    JobManagerCreationParametersBuilder setJobScratchIdentifier(String jobScratchIdentifier) {
        this.jobScratchIdentifier = jobScratchIdentifier
        return this;
    }

    JobManagerCreationParameters build() {
        return new JobManagerCreationParameters(createDaemon, updateInterval, userIdForJobQueries, trackUserJobsOnly, trackOnlyStartedJobs, jobIDIdentifier, jobArrayIDIdentifier, jobScratchIdentifier)
    }
}
