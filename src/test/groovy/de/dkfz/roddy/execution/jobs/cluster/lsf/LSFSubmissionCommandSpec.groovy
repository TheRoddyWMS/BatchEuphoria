/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.config.ResourceSetSize
import de.dkfz.roddy.execution.Code
import de.dkfz.roddy.execution.Command
import de.dkfz.roddy.execution.CommandI
import de.dkfz.roddy.execution.Executable
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.TestHelper
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.TimeUnit
import spock.lang.Specification

import java.nio.file.Paths

class LSFSubmissionCommandSpec extends Specification {

    LSFJobManager jobManager = new LSFJobManager(TestHelper.makeExecutionService(), JobManagerOptions.create().build())

    private BEJob makeJob(Map<String, String> mapOfParameters,
                          CommandI command = new Command(new Executable(Paths.get("/tmp/test.sh")),
                                                         ["\$someRemoteVariable"]),
                          String accountingProject = null) {
        BEJob job = new BEJob(
                null,
                jobManager,
                "Test",
                command,
                new ResourceSet(ResourceSetSize.l,
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
                null,
                accountingProject)
        job
    }

    def "assemble dependency string without dependencies" () throws Exception {
        when:
        def mapOfVars = ["a": "a", "b": "b"]
        BEJob job = makeJob(mapOfVars)
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob(mapOfVars),
                "jobName",
                null,
                mapOfVars,
                null)
        then:
        cmd.assembleDependencyParameter([]) == ""
    }

    def "assemble variable export parameters with no variables" () {
        when:
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob([:]),
                 "jobName",
                null,
                [:],
                null)

        then:
        cmd.assembleVariableExportParameters() == "-env \"none\""
    }

    def "assemble variable export parameters with only variables" () {
        when:
        Map<String, String> mapOfVars = ["a": "a", "b": null] as LinkedHashMap<String, String>
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob(mapOfVars),
                "jobName",
                null,
                mapOfVars,
                null)

        then:
        cmd.assembleVariableExportParameters() == "-env \"a=a, b\""
    }

    def "assemble variable export parameters with 'all' variables" () {
        when:
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob([:] as LinkedHashMap<String, String>),
                "jobName",
                null,
                [:],
                null)
        cmd.passEnvironment = Optional.of(true)

        then:
        cmd.assembleVariableExportParameters() == "-env \"all\""
    }

    def "assemble variable export parameters with 'all' and explicit variables" () {
        when:
        Map<String, String> mapOfVars = ["a": "a", "b": null] as LinkedHashMap<String, String>
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob(mapOfVars as LinkedHashMap<String, String>),
                "jobName",
                null,
                mapOfVars,
                null)
        cmd.passEnvironment = Optional.of(true)

        then:
        cmd.assembleVariableExportParameters() == "-env \"all, a=a, b\""
    }

    def "command without accounting name" () {
        when:
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob([:]),
                "jobname",
        null,
                [:],
                null)

        then:
        cmd.toBashCommandString() == 'LSB_NTRIES=5 bsub -env "none"  -J jobname -H -cwd "$HOME" -o /dev/null    -M 1024 -R "rusage[mem=1024]" -W 60 -n 4 -R "span[hosts=1]"    /tmp/test.sh \\$someRemoteVariable'
    }

    def "command with accounting name" () {
        when:
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob([:],
                        new Command(new Executable(Paths.get("/tmp/test.sh")),
                                                         ["\$someRemoteVariable"]),
                        "accountingProject"),
                "jobname",
                null,
                [:],
                null)

        then:
        cmd.toBashCommandString() == 'LSB_NTRIES=5 bsub -env "none" -P "accountingProject" -J jobname -H -cwd "$HOME" -o /dev/null    -M 1024 -R "rusage[mem=1024]" -W 60 -n 4 -R "span[hosts=1]"    /tmp/test.sh \\$someRemoteVariable'
    }

    def "submitting a script as code"() {
        given:
        Code code = new Code("echo 'Hello World'")
        when:
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob([:],
                        code),
                "jobname",
                null,
                [:],
                null)
        then:
        cmd.toBashCommandString() == "echo -e '#!/bin/bash\necho '\\''Hello World'\\''' | LSB_NTRIES=5 bsub -env \"none\"  -J jobname -H -cwd \"\$HOME\" -o /dev/null    -M 1024 -R \"rusage[mem=1024]\" -W 60 -n 4 -R \"span[hosts=1]\"   "
    }


}
