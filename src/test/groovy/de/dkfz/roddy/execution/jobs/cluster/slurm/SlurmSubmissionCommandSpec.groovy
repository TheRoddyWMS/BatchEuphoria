package de.dkfz.roddy.execution.jobs.cluster.slurm

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.config.ResourceSetSize
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

class SlurmSubmissionCommandSpec extends Specification {

    SlurmJobManager jobManager = new SlurmJobManager(TestHelper.makeExecutionService(), JobManagerOptions.create().build())

    private BEJob makeJob(
            Map<String, String> mapOfParameters,
            CommandI command,
            String accountingProject = null) {
        BEJob job = new BEJob(
                null,
                jobManager,
                "Test",
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
        def mapOfVars = ["a": "a", "b": "b"]
        BEJob job = makeJob(mapOfVars,
                            new Executable(Paths.get("/tmp/test.sh")))
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob(mapOfVars,
                        new Executable(Paths.get("/tmp/test.sh"))),
                "jobName",
                null, mapOfVars,
                null)
        then:
        cmd.assembleDependencyParameter([]) == ""
    }

    def "assemble variable export parameters with no variables"() {
        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob([:],
                        new Executable(Paths.get("/tmp/test.sh"))),
                "jobName",
                null,
                [:],
                null)
        then:
        cmd.assembleVariableExportParameters() == ""
    }

    def "assemble variable export parameters with only variables"() {
        when:
        Map<String, String> mapOfVars = ["a": "a", "b": null] as LinkedHashMap<String, String>
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob(mapOfVars,
                        new Executable(Paths.get("/tmp/test.sh"))),
                "jobName",
                null,
                mapOfVars,
                null)
        then:
        cmd.assembleVariableExportParameters() == "--export=\"a=a,b\""
    }

    def "assemble variable export parameters with 'all' variables"() {
        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob([:] as LinkedHashMap<String, String>,
                        new Executable(Paths.get("/tmp/test.sh"))),
                "jobName",
                null,
                [:],
                null)
        cmd.passEnvironment = Optional.of(true)
        then:
        cmd.assembleVariableExportParameters() == "--get-user-env "
    }

    def "assemble variable export parameters with 'all' and explicit variables"() {
        when:
        Map<String, String> mapOfVars = ["a": "a", "b": null] as LinkedHashMap<String, String>
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob(mapOfVars as LinkedHashMap<String, String>,
                        new Executable(Paths.get("/tmp/test.sh"))),
                "jobName",
                null,
                mapOfVars,
                null)
        cmd.passEnvironment = Optional.of(true)
        then:
        cmd.assembleVariableExportParameters() == "--get-user-env  --export=\"a=a,b\""
    }

    def "command without accounting name"() {
        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob([:],
                        new Executable(Paths.get("/tmp/test.sh"))),
                "jobname",
                null,
                [:],
                null)
        then:
        cmd.toBashCommandString() == 'sbatch   --job-name jobname --hold --chdir $HOME     --mem=1024M   --time=1:00:00   --nodes=1  --cores-per-socket=4  --parsable --kill-on-invalid-dep=yes --propagate=none  /tmp/test.sh'
    }

    def "command with accounting name"() {
        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob([:],
                        new Executable(Paths.get("/tmp/test.sh")),
                        "accountingProject"),
                "jobname",
                null,
                [:],
                null)
        then:
        cmd.toBashCommandString() == 'sbatch  --account="accountingProject" --job-name jobname --hold --chdir $HOME     --mem=1024M   --time=1:00:00   --nodes=1  --cores-per-socket=4  --parsable --kill-on-invalid-dep=yes --propagate=none  /tmp/test.sh'
    }

    def "submit script as code"() {
        given:
        BEJob job = makeJob([:],
                            new Code("echo 'Hello World'\n"))

        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                job,
                "jobname",
                null,
                [:],
                null)
        then:
        cmd.toBashCommandString() == "echo -ne \\#'!'/bin/bash\\\\necho\\ \\'Hello\\ World\\'\\\\n | sbatch   --job-name jobname --hold --chdir \$HOME     --mem=1024M   --time=1:00:00   --nodes=1  --cores-per-socket=4  --parsable --kill-on-invalid-dep=yes --propagate=none  /dev/stdin"

    }

}
