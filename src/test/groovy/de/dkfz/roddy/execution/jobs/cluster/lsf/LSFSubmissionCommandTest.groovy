/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.config.ResourceSetSize
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.TestHelper
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic
import org.junit.Before
import org.junit.Test

@CompileStatic
class LSFSubmissionCommandTest {


    LSFJobManager jobManager

    @Before
    void setUp() throws Exception {
        jobManager = new LSFJobManager(TestHelper.makeExecutionService(), JobManagerOptions.create().build())
    }

    private BEJob makeJob(Map<String, String> mapOfParameters) {
        BEJob job = new BEJob(null, "Test", new File("/tmp/test.sh"), null, null, new ResourceSet(ResourceSetSize.l, new BufferValue(1, BufferUnit.G), 4, 1, new TimeUnit("1h"), null, null, null), [], mapOfParameters, jobManager, JobLog.none(), null)
        job
    }

    @Test
    void testAssembleDependencyStringWithoutDependencies() throws Exception {
        def mapOfVars = ["a": "a", "b": "b"]
        BEJob job = makeJob(mapOfVars)
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(jobManager, makeJob(mapOfVars),
                "jobName", null, mapOfVars, null, "/tmp/test.sh")
        assert cmd.assembleDependencyParameter([]) == ""
    }

    @Test
    void testAssembleVariableExportParameters_nothing() {
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(jobManager, makeJob([:]),
                 "jobName", null, [:], null, "/tmp/test.sh")
        assert cmd.assembleVariableExportParameters() == "-env \"none\""
    }

    @Test
    void testAssembleVariableExportParameters_onlyVars() {
        Map<String, String> mapOfVars = ["a": "a", "b": null] as LinkedHashMap<String, String>
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(jobManager, makeJob(mapOfVars),
                "jobName", null, mapOfVars, null, "/tmp/test.sh")
        assert cmd.assembleVariableExportParameters() == "-env \"a=a, b\""
    }

    @Test
    void testAssembleVariableExportParameters_allVars() {
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(jobManager, makeJob([:] as LinkedHashMap<String, String>),
                "jobName", null, [:], null, "/tmp/test.sh")
        cmd.passEnvironment = Optional.of(true)
        assert cmd.assembleVariableExportParameters() == "-env \"all\""
    }

    @Test
    void testAssembleVariableExportParameters_allVarsAndExplicit() {
        Map<String, String> mapOfVars = ["a": "a", "b": null] as LinkedHashMap<String, String>
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(jobManager, makeJob(mapOfVars as LinkedHashMap<String, String>),
                "jobName", null, mapOfVars, null, "/tmp/test.sh")
        cmd.passEnvironment = Optional.of(true)
        assert cmd.assembleVariableExportParameters() == "-env \"all, a=a, b\""
    }
}
