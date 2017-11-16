/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import groovy.transform.CompileStatic

/**
 * Created by heinold on 28.02.17.
 */
@CompileStatic
class JobManagerCreationParameters {

    boolean strictMode
    boolean createDaemon = BatchEuphoriaJobManager.JOBMANAGER_DEFAULT_CREATE_DAEMON
    int updateInterval = BatchEuphoriaJobManager.JOBMANAGER_DEFAULT_UPDATEINTERVAL

    String userIdForJobQueries = ""
    boolean trackUserJobsOnly = BatchEuphoriaJobManager.JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY
    boolean trackOnlyStartedJobs = BatchEuphoriaJobManager.JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY

    String userGroup
    String userAccount
    String userEmail
    String userMask

    JobManagerCreationParameters() {
    }

    JobManagerCreationParameters(boolean createDaemon, int updateInterval, String userIdForJobQueries, boolean trackUserJobsOnly,
                                 boolean trackOnlyStartedJobs, String userAccount, String userEmail, String userMask, String userGroup,
                                 boolean strictMode) {
        this.createDaemon = createDaemon
        this.updateInterval = updateInterval
        this.userIdForJobQueries = userIdForJobQueries
        this.trackUserJobsOnly = trackUserJobsOnly
        this.trackOnlyStartedJobs = trackOnlyStartedJobs
        this.userMask = userMask
        this.userGroup = userGroup
        this.userEmail = userEmail
        this.userAccount = userAccount
        this.strictMode = strictMode
    }
}
