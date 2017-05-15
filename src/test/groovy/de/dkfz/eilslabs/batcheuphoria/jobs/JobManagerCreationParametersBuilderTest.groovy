/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs

import groovy.transform.CompileStatic
import org.junit.Test

/**
 * Created by heinold on 28.02.17.
 */
@CompileStatic
class JobManagerCreationParametersBuilderTest {

    @Test
    void testDefaults() {
        def parms = new JobManagerCreationParametersBuilder().build()
        assert parms.jobScratchIdentifier == JobManager.BE_DEFAULT_JOBSCRATCH
        assert parms.jobIDIdentifier == JobManager.BE_DEFAULT_JOBID
        assert parms.jobArrayIDIdentifier == JobManager.BE_DEFAULT_JOBARRAYINDEX
        assert parms.trackOnlyStartedJobs == JobManager.JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY
        assert parms.trackUserJobsOnly == JobManager.JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY
        assert parms.userIdForJobQueries == ""
        assert parms.updateInterval == JobManager.JOBMANAGER_DEFAULT_UPDATEINTERVAL
    }

    @Test
    void testBuild() {
        JobManagerCreationParameters parms = new JobManagerCreationParametersBuilder()
                .setJobIDIdentifier("SOMETHING")
                .setJobArrayIDIdentifier("SOMETHINGB")
                .setJobScratchIdentifier("SCRAT")
                .setUserIdForJobQueries("BLA")
                .setJobScratchIdentifier("SCRAT")
                .setTrackUserJobsOnly(false).build()

        assert parms instanceof JobManagerCreationParameters
        assert parms.jobScratchIdentifier == "SCRAT"
        assert parms.jobIDIdentifier == "SOMETHING"
        assert parms.jobArrayIDIdentifier == "SOMETHINGB"
        assert parms.trackOnlyStartedJobs == JobManager.JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY
        assert parms.trackUserJobsOnly == JobManager.JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY
        assert parms.userIdForJobQueries == "BLA"
        assert parms.updateInterval == JobManager.JOBMANAGER_DEFAULT_UPDATEINTERVAL
    }
}