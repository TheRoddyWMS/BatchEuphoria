/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import groovy.transform.CompileStatic
import groovy.transform.builder.Builder
import groovy.transform.builder.ExternalStrategy

import java.time.Duration


@CompileStatic
class JobManagerOptions {

    boolean strictMode
    boolean createDaemon
    Duration updateInterval

    String userIdForJobQueries
    boolean trackUserJobsOnly
    boolean trackOnlyStartedJobs

    String userGroup
    String userAccount
    String userEmail
    String userMask

    static JobManagerOptionsBuilder create() {
        new JobManagerOptionsBuilder()
    }
}

@Builder(builderStrategy=ExternalStrategy, forClass=JobManagerOptions, prefix = "set")
class JobManagerOptionsBuilder {
    JobManagerOptionsBuilder() {
        trackUserJobsOnly = false
        trackOnlyStartedJobs = false
        userIdForJobQueries = "123"
        strictMode = true
        updateInterval = Duration.ofMinutes(5)
        createDaemon = false
    }
}
