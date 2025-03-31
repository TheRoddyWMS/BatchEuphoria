/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.config.ResourceSetSize
import de.dkfz.roddy.tools.EscapableString
import de.dkfz.roddy.tools.BashInterpreter
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

import static de.dkfz.roddy.tools.EscapableString.Shortcuts.*

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

    private BEJob makeJob(Map<String, EscapableString> mapOfParameters) {
        BEJob job = new BEJob(
                null,
                jobManager,
                u("Test"),
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
                u("jobName"),
                null,
                [:] as Map<String, EscapableString>)
        assert cmd.assembleVariableExportParameters() == c()
    }

    @Test
    void testAssembleVariableExportParameters_onlyVars() {
        Map<String, EscapableString> mapOfVars =
                ["a": u("a"), "b": null] as LinkedHashMap<String, EscapableString>
        PBSSubmissionCommand cmd = new PBSSubmissionCommand(
                jobManager,
                makeJob(mapOfVars),
                u("jobName"),
                null,
                mapOfVars)
        assert cmd.assembleVariableExportParameters() ==
               c(u("-v "), u("a"), e("="), u("a"), u(","), u("b"))
    }

    @Test
    void testAssembleVariableExportParameters_allVars() {
        PBSSubmissionCommand cmd = new PBSSubmissionCommand(
                jobManager,
                makeJob([:] as LinkedHashMap<String, EscapableString>),
                u("jobName"),
                null,
                [:] as Map<String, EscapableString>)
        cmd.passEnvironment = Optional.of(true)
        assert cmd.assembleVariableExportParameters() == c(u("-V"))
    }

    @Test
    void testAssembleVariableExportParameters_allVarsAndExplicit() {
        Map<String, EscapableString> mapOfVars =
                ["a": e("a!"), "b": null] as LinkedHashMap<String, EscapableString>
        PBSSubmissionCommand cmd = new PBSSubmissionCommand(
                jobManager,
                makeJob(mapOfVars),
                u("jobName"),
                null,
                mapOfVars)
        cmd.passEnvironment = Optional.of(true)
        assert BashInterpreter.instance.interpret(cmd.assembleVariableExportParameters()) ==
               "-V -v a\\=a'!',b"
    }
}
