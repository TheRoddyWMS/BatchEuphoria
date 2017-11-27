/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.batcheuphoria.jobs

import de.dkfz.roddy.execution.jobs.JobManagerOptions
import groovy.transform.CompileStatic
import org.junit.Test

import java.time.Duration

/**
 * Created by heinold on 28.02.17.
 */
@CompileStatic
class JobManagerOptionsTest {

    @Test
    void testDefaults() {
        def parms = JobManagerOptions.create().build()
        assert parms.trackOnlyStartedJobs == false
        assert parms.trackUserJobsOnly == false
        assert parms.userIdForJobQueries == "123"
        assert parms.updateInterval == Duration.ofMinutes(5)
    }
}