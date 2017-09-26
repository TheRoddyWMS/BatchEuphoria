/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.batcheuphoria.jobs

import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.JobManagerCreationParameters
import de.dkfz.roddy.execution.jobs.JobManagerCreationParametersBuilder
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
        assert parms.trackOnlyStartedJobs == BatchEuphoriaJobManager.JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY
        assert parms.trackUserJobsOnly == BatchEuphoriaJobManager.JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY
        assert parms.userIdForJobQueries == ""
        assert parms.updateInterval == BatchEuphoriaJobManager.JOBMANAGER_DEFAULT_UPDATEINTERVAL
    }

    @Test
    void testBuild() {
        JobManagerCreationParameters parms = new JobManagerCreationParametersBuilder()
                .setUserIdForJobQueries("BLA")
                .setTrackUserJobsOnly(false).build()

        assert parms instanceof JobManagerCreationParameters
        assert parms.trackOnlyStartedJobs == BatchEuphoriaJobManager.JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY
        assert parms.trackUserJobsOnly == BatchEuphoriaJobManager.JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY
        assert parms.userIdForJobQueries == "BLA"
        assert parms.updateInterval == BatchEuphoriaJobManager.JOBMANAGER_DEFAULT_UPDATEINTERVAL
    }
}