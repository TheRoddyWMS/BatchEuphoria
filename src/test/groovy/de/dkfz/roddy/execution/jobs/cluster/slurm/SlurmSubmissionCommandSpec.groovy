package de.dkfz.roddy.execution.jobs.cluster.slurm

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.config.ResourceSetSize
import de.dkfz.roddy.tools.EscapableString
import de.dkfz.roddy.execution.Code
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

import static de.dkfz.roddy.tools.EscapableString.Shortcuts.*

class SlurmSubmissionCommandSpec extends Specification {

    SlurmJobManager jobManager = new SlurmJobManager(TestHelper.makeExecutionService(), JobManagerOptions.create().build())

    private BEJob makeJob(
            Map<String, EscapableString> mapOfParameters,
            CommandI command,
            EscapableString accountingProject = null) {
        BEJob job = new BEJob(
                null,
                jobManager,
                u("Test"),
                command,
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
                null,
                accountingProject)
        job
    }

    def "assemble dependency string without dependencies"() throws Exception {
        when:
        Map<String, EscapableString> mapOfVars = ["a": u("a"), "b": u("b")]
        BEJob job = makeJob(mapOfVars,
                            new Executable(Paths.get("/tmp/test.sh")))
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob(mapOfVars,
                        new Executable(Paths.get("/tmp/test.sh"))),
                u("jobName"),
                null,
                mapOfVars)
        then:
        cmd.assembleDependencyParameter([]) == c()
    }

    def "assemble variable export parameters with no variables"() {
        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob([:] as Map<String, EscapableString>,
                        new Executable(Paths.get("/tmp/test.sh"))),
                u("jobName"),
                null,
                [:])
        then:
        cmd.assembleVariableExportParameters() == c()
    }

    def "assemble variable export parameters with only variables"() {
        when:
        Map<String, EscapableString> mapOfVars = ["a": u("a"), "b": null]
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob(mapOfVars,
                        new Executable(Paths.get("/tmp/test.sh"))),
                u("jobName"),
                null,
                mapOfVars)
        then:
        cmd.assembleVariableExportParameters() ==
            c(u("--export="), u("a"), e("="), u("a"), u(","), u("b"))
    }

    def "assemble variable export parameters with 'all' variables"() {
        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob([:] as LinkedHashMap<String, EscapableString>,
                        new Executable(Paths.get("/tmp/test.sh"))),
                u("jobName"),
                null,
                [:])
        cmd.passEnvironment = Optional.of(true)
        then:
        cmd.assembleVariableExportParameters() == c(u("--get-user-env "))
    }

    def "assemble variable export parameters with 'all' and explicit variables"() {
        when:
        Map<String, EscapableString> mapOfVars = ["a": u("a"), "b": null]
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob(mapOfVars as LinkedHashMap<String, String>,
                        new Executable(Paths.get("/tmp/test.sh"))),
                u("jobName"),
                null,
                mapOfVars)
        cmd.passEnvironment = Optional.of(true)
        then:
        cmd.assembleVariableExportParameters() ==
            c(u("--get-user-env "), u("--export="), u("a"), e("="), u("a"), u(","), u("b"))
    }

    def "command without accounting name"() {
        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob([:] as Map<String, EscapableString>,
                        new Executable(Paths.get("/tmp/test.sh"))),
                u("jobname"),
                null,
                [:])
        then:
        cmd.toBashCommandString() == 'sbatch --job-name jobname --hold --chdir "$HOME"  --mem=1024M   --time=1:00:00   --nodes=1  --cores-per-socket=4 --parsable --kill-on-invalid-dep=yes --propagate=NONE  /tmp/test.sh'
    }

    def "command with accounting name"() {
        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob([:] as Map<String, EscapableString>,
                        new Executable(Paths.get("/tmp/test.sh")),
                        u("accountingProject")),
                u("jobname"),
                null,
                [:])
        then:
        cmd.toBashCommandString() == 'sbatch --account=accountingProject --job-name jobname --hold --chdir "$HOME"  --mem=1024M   --time=1:00:00   --nodes=1  --cores-per-socket=4 --parsable --kill-on-invalid-dep=yes --propagate=NONE  /tmp/test.sh'
    }

    def "submit script as code"() {
        given:
        BEJob job = makeJob([:],
                            new Code("echo 'Hello World'\n"))

        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                job,
                u("jobname"),
                null,
                [:])
        then:
        cmd.toBashCommandString() == "echo -ne \\#'!'/bin/bash\\\\necho\\ \\'Hello\\ World\\'\\\\n | sbatch --job-name jobname --hold --chdir \"\$HOME\"  --mem=1024M   --time=1:00:00   --nodes=1  --cores-per-socket=4 --parsable --kill-on-invalid-dep=yes --propagate=NONE  /dev/stdin"

    }

}
