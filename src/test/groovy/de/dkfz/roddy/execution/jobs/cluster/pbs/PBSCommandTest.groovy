/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.config.ResourceSetSize
import de.dkfz.roddy.execution.Executable
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.TestHelper
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic
import org.junit.Before
import org.junit.Test

import java.nio.file.Paths

/**
 * Created by heinold on 26.03.17.
 */
@CompileStatic
class PBSCommandTest {


    PBSJobManager jobManager

    @Before
    void setUp() throws Exception {
        jobManager = new PBSJobManager(TestHelper.makeExecutionService(), JobManagerOptions.create().build())
    }

    private BEJob makeJob(Map<String, String> mapOfParameters) {
        BEJob job = new BEJob(
                null,
                jobManager,
                "Test",
                new Executable(Paths.get("/tmp/test.sh")),
                new ResourceSet(
                        ResourceSetSize.l,
                        new BufferValue(1, BufferUnit.G),
                        4,
                        1,
                        new TimeUnit("1h"),
                        null,
                        null,
                        null),
                [],
                mapOfParameters,
                JobLog.none(),
                null)
        job
    }

    @Test
    void testAssembleVariableExportParameters_nothing() {
        PBSSubmissionCommand cmd = new PBSSubmissionCommand(
                jobManager,
                makeJob([:]),
                "jobName",
                null,
                [:],
                null,
                new Executable(Paths.get("/tmp/test.sh")))
        assert cmd.assembleVariableExportParameters() == ""
    }

    @Test
    void testAssembleVariableExportParameters_onlyVars() {
        Map<String, String> mapOfVars = ["a": "a", "b": null] as LinkedHashMap<String, String>
        PBSSubmissionCommand cmd = new PBSSubmissionCommand(
                jobManager,
                makeJob(mapOfVars),
                "jobName",
                null,
                mapOfVars,
                null,
                new Executable(Paths.get("/tmp/test.sh")))
        assert cmd.assembleVariableExportParameters() == "-v \"a=a,b\""
    }

    @Test
    void testAssembleVariableExportParameters_allVars() {
        PBSSubmissionCommand cmd = new PBSSubmissionCommand(
                jobManager,
                makeJob([:] as LinkedHashMap<String, String>),
                "jobName",
                null,
                [:],
                null,
                new Executable(Paths.get("/tmp/test.sh")))
        cmd.passEnvironment = Optional.of(true)
        assert cmd.assembleVariableExportParameters() == "-V"
    }

    @Test
    void testAssembleVariableExportParameters_allVarsAndExplicit() {
        Map<String, String> mapOfVars = ["a": "a", "b": null] as LinkedHashMap<String, String>
        PBSSubmissionCommand cmd = new PBSSubmissionCommand(
                jobManager,
                makeJob(mapOfVars as LinkedHashMap<String, String>),
                "jobName",
                null,
                mapOfVars,
                null,
                new Executable(Paths.get("/tmp/test.sh")))
        cmd.passEnvironment = Optional.of(true)
        assert cmd.assembleVariableExportParameters() == "-V -v \"a=a,b\""
    }
}
