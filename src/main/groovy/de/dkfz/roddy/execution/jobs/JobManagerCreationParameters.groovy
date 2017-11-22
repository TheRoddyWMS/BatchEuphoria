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

    public static final int JOBMANAGER_DEFAULT_UPDATEINTERVAL = 300
    public static final boolean JOBMANAGER_DEFAULT_CREATE_DAEMON = true
    public static final boolean JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY = false
    public static final boolean JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY = false

    boolean strictMode
    boolean createDaemon = JOBMANAGER_DEFAULT_CREATE_DAEMON
    int updateInterval = JOBMANAGER_DEFAULT_UPDATEINTERVAL

    String userIdForJobQueries = ""
    boolean trackUserJobsOnly = JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY
    boolean trackOnlyStartedJobs = JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY

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
