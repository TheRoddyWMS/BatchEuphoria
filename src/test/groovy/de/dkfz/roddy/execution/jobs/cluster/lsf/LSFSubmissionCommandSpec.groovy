/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.config.ResourceSetSize
import de.dkfz.roddy.execution.*
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.TestHelper
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.TimeUnit
import spock.lang.Specification

import java.nio.file.Paths

import static de.dkfz.roddy.execution.EscapableString.*

class LSFSubmissionCommandSpec extends Specification {

    LSFJobManager jobManager = new LSFJobManager(TestHelper.makeExecutionService(), JobManagerOptions.create().build())

    private BEJob makeJob(Map<String, AnyEscapableString> mapOfParameters,
                          CommandI command = new Command(new Executable(Paths.get("/tmp/test.sh")),
                                                         [u("\$someRemoteVariable")]),
                          // Here, someRemoteVariable is *not* quoted. The Command should take the values,
                          // like they are supposed to be used at the call site.
                          AnyEscapableString accountingProject = null) {
        BEJob job = new BEJob(
                null,
                jobManager,
                u("Test"),
                command,
                new ResourceSet(ResourceSetSize.l,
                                new BufferValue(1, BufferUnit.G),
                                4,
                                1,
                                new TimeUnit("1h"),
                                null,
                                null,
                                null),
                [new BEJob(new BEJobID("a"), jobManager, u("a")),
                 new BEJob(new BEJobID("b"), jobManager, u("b"))],
                mapOfParameters,
                JobLog.none(),
                null,
                accountingProject)
        job
    }

    def "assemble dependency string without dependencies" () throws Exception {
        when:
        Map<String, AnyEscapableString> mapOfVars = ["a": u("a"), "b": u("b")]
        BEJob job = makeJob(mapOfVars)
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob(mapOfVars),
                u("jobName"),
                null,
                mapOfVars)
        then:
        cmd.assembleDependencyParameter([]) == c()
    }

    def "assemble variable export parameters with no variables" () {
        when:
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob([:]),
                 u("jobName"),
                null,
                [:])

        then:
        cmd.assembleVariableExportParameters() ==
            c(u("-env"), u(" "), u("none"))
    }

    def "assemble variable export parameters with only variables" () {
        when:
        Map<String, AnyEscapableString> mapOfVars =
                ["a": e("a"), "b": null]
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob(mapOfVars),
                u("jobName"),
                null,
                mapOfVars)

        then:
        cmd.assembleVariableExportParameters() ==
            c(u("-env"), u(" "), u("a"), u("="), e("a"), e(", "), u("b"))
    }

    def "assemble variable export parameters with 'all' variables" () {
        when:
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob([:] as LinkedHashMap<String, AnyEscapableString>),
                u("jobName"),
                null,
                [:])
        cmd.passEnvironment = Optional.of(true)

        then:
        cmd.assembleVariableExportParameters() == c(u("-env"), u(" "), u("all"))
    }

    def "assemble variable export parameters with 'all' and explicit variables" () {
        when:
        Map<String, AnyEscapableString> mapOfVars = ["a": u("a"), "b": null]
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob(mapOfVars),
                u("jobName"),
                null,
                mapOfVars)
        cmd.passEnvironment = Optional.of(true)

        then:
        cmd.assembleVariableExportParameters() ==
            c(u("-env"), u(" "), u("all"), e(", "), u("a"), u("="), u("a"), e(", "), u("b"))
    }

    def "command without accounting name" () {
        when:
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob([:]),
                u("jobname"),
        null,
                [:])

        then:
        cmd.toBashCommandString() == 'LSB_NTRIES=5 bsub -env none  -J jobname -H -cwd "$HOME" -o /dev/null  -M 1024 -R rusage\\[mem\\=1024] -R span\\[hosts\\=1] -W 60 -n 4 -ti -w  done\\(a\\)\\ \\&\\&\\ done\\(b\\)   /tmp/test.sh\\ \\$someRemoteVariable'
    }

    def "command with accounting name" () {
        when:
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob([:],
                        new Command(new Executable(Paths.get("/tmp/test.sh")),
                                    [u("\$someRemoteVariable")]),
                        u("accountingProject")),
                u("jobname"),
                null,
                [:])

        then:
        cmd.toBashCommandString() == 'LSB_NTRIES=5 bsub -env none -P accountingProject -J jobname -H -cwd "$HOME" -o /dev/null  -M 1024 -R rusage\\[mem\\=1024] -R span\\[hosts\\=1] -W 60 -n 4 -ti -w  done\\(a\\)\\ \\&\\&\\ done\\(b\\)   /tmp/test.sh\\ \\\$someRemoteVariable'
    }

    def "submitting a script as code"() {
        given:
        Code code = new Code("echo 'Hello World';\\n")
        when:
        LSFSubmissionCommand cmd = new LSFSubmissionCommand(
                jobManager,
                makeJob([:],
                        code),
                u("jobname"),
                null,
                [:])
        then:
        cmd.toBashCommandString() == "echo -ne \\#'!'/bin/bash\\\\necho\\ \\'Hello\\ World\\'\\;\\\\n | LSB_NTRIES=5 bsub -env none  -J jobname -H -cwd \"\$HOME\" -o /dev/null  -M 1024 -R rusage\\[mem\\=1024] -R span\\[hosts\\=1] -W 60 -n 4 -ti -w  done\\(a\\)\\ \\&\\&\\ done\\(b\\)  "
    }


}
