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

    /**
     * Request memory per job
     */
    boolean requestMemoryIsEnabled

    /**
     * Request walltime per job
     */
    boolean requestWalltimeIsEnabled

    /**
     * Request queue for a job
     */
    boolean requestQueueIsEnabled

    /**
     * Request cores per job (also effects node request)
     */
    boolean requestCoresIsEnabled

    /**
     * Request storage per job
     */
    boolean requestStorageIsEnabled

    /**
     * Should the manager pass the current (local) environment to the execution host? By default this is 'None'
     */
    Optional<Boolean> passEnvironment

    Map<String, String> additionalOptions

    static JobManagerOptionsBuilder create() {
        new JobManagerOptionsBuilder()
    }

    Map<String, String> getAdditionalOptions() {
        // Never return a null map.
        return additionalOptions ?: (new LinkedHashMap<String, String>() as Map<String, String>)
    }
}

@Builder(builderStrategy = ExternalStrategy, forClass = JobManagerOptions, prefix = "set")
class JobManagerOptionsBuilder {
    JobManagerOptionsBuilder() {
        trackUserJobsOnly = false
        trackOnlyStartedJobs = false
        strictMode = true
        updateInterval = Duration.ofMinutes(5)
        createDaemon = false
        requestMemoryIsEnabled = true
        requestWalltimeIsEnabled = true
        requestQueueIsEnabled = true
        requestCoresIsEnabled = true
        requestStorageIsEnabled = false  // Defaults to false, not supported now.
        // TODO: The following is a quickfix for the LSF -env problem. Change back to the commented version, when the cluster is fixed.
        // passEnvironment = Optional.empty()
        passEnvironment = Optional.of(true)   // TODO: Remove quickfix as soon as cluster is fixed.
        additionalOptions = [:]
    }
}
