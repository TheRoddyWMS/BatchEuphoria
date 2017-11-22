/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.batcheuphoria.jobs

import de.dkfz.roddy.execution.jobs.JobManagerCreationParameters
import groovy.transform.CompileStatic
import org.junit.Test

/**
 * Created by heinold on 28.02.17.
 */
@CompileStatic
class JobManagerCreationParametersTest {

    @Test
    void testDefaults() {

        "done(11030)&&done(11032)".tokenize(/&/).collect { it.find(/\d+/) }.each {
            println it
        }
        "done(11030)".tokenize(/&/).collect { it.find(/\d+/) }.each {
            println it
        }
        "done(11030) && done(11032)".tokenize(/&/).collect { it.find(/\d+/) }.each {
            println it
        }
        def parms = new JobManagerCreationParameters();
        assert parms.trackOnlyStartedJobs == JobManagerCreationParameters.JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY
        assert parms.trackUserJobsOnly == JobManagerCreationParameters.JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY
        assert parms.userIdForJobQueries == ""
        assert parms.updateInterval == JobManagerCreationParameters.JOBMANAGER_DEFAULT_UPDATEINTERVAL
    }
}