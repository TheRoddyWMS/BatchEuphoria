/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import static BatchEuphoriaJobManager.*
import groovy.transform.CompileStatic

/**
 * Created by heinold on 28.02.17.
 *
 * Remove. The class is basically a copy of the JobManagerCreationParameter with added fluent setters.
 */
@Deprecated
@CompileStatic
class JobManagerCreationParametersBuilder {

    boolean createDaemon = JOBMANAGER_DEFAULT_CREATE_DAEMON
    int updateInterval = JOBMANAGER_DEFAULT_UPDATEINTERVAL
    String userIdForJobQueries = ""
    boolean trackUserJobsOnly = JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY
    boolean trackOnlyStartedJobs = JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY
    String userGroup = null
    String userAccount = null
    String userEmail = null
    String userMask = null
    boolean strictMode = false

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

    JobManagerCreationParametersBuilder setUserGroup(String userGroup) {
        this.userGroup = userGroup
        return this
    }

    JobManagerCreationParametersBuilder setUserAccount(String userAccount) {
        this.userAccount = userAccount
        return this
    }

    JobManagerCreationParametersBuilder setUserEmail(String userEmail) {
        this.userEmail = userEmail
        return this
    }

    JobManagerCreationParametersBuilder setUserMask(String userMask) {
        this.userMask = userMask
        return this
    }

    JobManagerCreationParameters build() {
        return new JobManagerCreationParameters(createDaemon, updateInterval, userIdForJobQueries, trackUserJobsOnly, trackOnlyStartedJobs, userAccount, userEmail, userMask, userGroup, strictMode)
    }
}
