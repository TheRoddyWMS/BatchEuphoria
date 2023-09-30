package de.dkfz.roddy.execution.jobs.cluster.slurm

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
import spock.lang.Specification

import java.nio.file.Paths

class SlurmSubmissionCommandSpec extends Specification {

    SlurmJobManager jobManager = new SlurmJobManager(TestHelper.makeExecutionService(), JobManagerOptions.create().build())

    private BEJob makeJob(Map<String, String> mapOfParameters, String accountingProject = null) {
        BEJob job = new BEJob(
                null,
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
                jobManager,
                JobLog.none(),
                null,
                accountingProject)
        job
    }

    def "assemble dependency string without dependencies"() throws Exception {
        when:
        def mapOfVars = ["a": "a", "b": "b"]
        BEJob job = makeJob(mapOfVars)
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob(mapOfVars),
                "jobName",
                null, mapOfVars,
                null,
                new Executable(Paths.get("/tmp/test.sh")))
        then:
        cmd.assembleDependencyParameter([]) == ""
    }

    def "assemble variable export parameters with no variables"() {
        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob([:]),
                "jobName",
                null,
                [:],
                null,
                new Executable(Paths.get("/tmp/test.sh")))
        then:
        cmd.assembleVariableExportParameters() == ""
    }

    def "assemble variable export parameters with only variables"() {
        when:
        Map<String, String> mapOfVars = ["a": "a", "b": null] as LinkedHashMap<String, String>
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob(mapOfVars),
                "jobName",
                null,
                mapOfVars,
                null,
                new Executable(Paths.get("/tmp/test.sh")))
        then:
        cmd.assembleVariableExportParameters() == "--export=\"a=a,b\""
    }

    def "assemble variable export parameters with 'all' variables"() {
        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob([:] as LinkedHashMap<String, String>),
                "jobName",
                null,
                [:],
                null,
                new Executable(Paths.get("/tmp/test.sh")))
        cmd.passEnvironment = Optional.of(true)
        then:
        cmd.assembleVariableExportParameters() == "--get-user-env "
    }

    def "assemble variable export parameters with 'all' and explicit variables"() {
        when:
        Map<String, String> mapOfVars = ["a": "a", "b": null] as LinkedHashMap<String, String>
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob(mapOfVars as LinkedHashMap<String, String>),
                "jobName",
                null,
                mapOfVars,
                null,
                new Executable(Paths.get("/tmp/test.sh")))
        cmd.passEnvironment = Optional.of(true)
        then:
        cmd.assembleVariableExportParameters() == "--get-user-env  --export=\"a=a,b\""
    }

    def "command without accounting name"() {
        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob([:]),
                "jobname",
                null,
                [:],
                null,
                new Executable(Paths.get("/tmp/test.sh")))
        then:
        cmd.toBashCommandString() == 'sbatch   --job-name jobname --hold --chdir $HOME     --mem=1024M   --time=1:00:00   --nodes=1  --cores-per-socket=4  --parsable --kill-on-invalid-dep=yes --propagate=none  /tmp/test.sh'
    }

    def "command with accounting name"() {
        when:
        SlurmSubmissionCommand cmd = new SlurmSubmissionCommand(
                jobManager,
                makeJob([:],
                        "accountingProject"),
                "jobname",
                null,
                [:],
                null,
                new Executable(Paths.get("/tmp/test.sh")))
        then:
        cmd.toBashCommandString() == 'sbatch  --account="accountingProject" --job-name jobname --hold --chdir $HOME     --mem=1024M   --time=1:00:00   --nodes=1  --cores-per-socket=4  --parsable --kill-on-invalid-dep=yes --propagate=none  /tmp/test.sh'
    }

}
