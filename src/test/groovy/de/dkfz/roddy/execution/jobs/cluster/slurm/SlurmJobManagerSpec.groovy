package de.dkfz.roddy.execution.jobs.cluster.slurm

import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.execution.jobs.TestHelper
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Duration

class SlurmJobManagerSpec extends Specification {

    SlurmJobManager jobManager = new SlurmJobManager(TestHelper.makeExecutionService(), JobManagerOptions.create().build())


    static final File getResourceFile(String file) {
        new File("src/test/resources/de/dkfz/roddy/execution/jobs/cluster/slurm/", file)
    }

    @Unroll
    def "test safelyParseColonSeparatedDuration with input '#value'"() {
        expect:
        SlurmJobManager.safelyParseColonSeparatedDuration(value) == Duration.parse(result)

        where:
        value         | result
        "12-00:00:00" | "PT288H"
        "12-12:30:15" | "PT300H30M15S"
        "12:00:00"    | "PT12H"
        "01:30:15"    | "PT1H30M15S"
    }

    def "test processExtendedOutput with scontrol.txt"() {
        when:
        GenericJobInfo jobInfo = jobManager.processExtendedOutput(getResourceFile("scontrol.txt").text)

        then:
        /** Directories and files */
        jobInfo.inputFile == new File("/dev/null")
        jobInfo.logFile == new File("/path/to/outputFile")
        jobInfo.user == "user(192456)"
        jobInfo.submissionHost == "compute038"
        jobInfo.executionHosts ==  ["compute038"]
        jobInfo.errorLogFile ==  new File("/path/to/errorFile")
        jobInfo.execHome ==  "/home/user"

        /** Status info */
        jobInfo.jobState == JobState.RUNNING
        jobInfo.exitCode == 0
        jobInfo.pendReason == "None"

        /** Resources */
        jobInfo.runTime == Duration.parse("PT3H21M58S")
        jobInfo.askedResources.mem == new BufferValue(300, BufferUnit.G, BufferUnit.K)
        jobInfo.askedResources.cores == 76
        jobInfo.askedResources.nodes == 1
        jobInfo.askedResources.walltime == Duration.parse("PT20H")
        jobInfo.askedResources.queue  == "compute"
        jobInfo.usedResources.mem == new BufferValue(300, BufferUnit.G, BufferUnit.K)
        jobInfo.usedResources.cores == 76
        jobInfo.usedResources.nodes == 1
        jobInfo.usedResources.walltime == Duration.parse("PT3H21M58S")
        jobInfo.usedResources.queue  == "compute"
        jobInfo.submitTime.toString().startsWith("2023-06-20T07:07:33")
        jobInfo.eligibleTime.toString().startsWith("2023-06-20T07:07:33")
        jobInfo.startTime.toString().startsWith("2023-06-20T07:07:34")
        jobInfo.endTime.toString().startsWith("2023-06-21T03:07:34")
    }

    def "test processExtendedOutputFromJson with sacct.json"() {
        when:
        GenericJobInfo jobInfo = jobManager.processExtendedOutputFromJson(getResourceFile("sacct.json").text)

        then:
        /** Common */
        jobInfo.user == "user"
        jobInfo.userGroup == "group"
        jobInfo.jobGroup == "group"
        jobInfo.priority == "0"
        jobInfo.executionHosts == ["compute013"]

        /** Status info */
        jobInfo.jobState == JobState.COMPLETED_SUCCESSFUL
        jobInfo.exitCode == 0
        jobInfo.pendReason == "Dependency"

        /** Resources */
        jobInfo.askedResources.mem == new BufferValue(7168, BufferUnit.M, BufferUnit.K)
        jobInfo.askedResources.cores == 1
        jobInfo.askedResources.nodes == 1
        jobInfo.askedResources.walltime == Duration.parse("PT5H")
        jobInfo.askedResources.queue  == "compute"
        jobInfo.usedResources.mem == new BufferValue(7168, BufferUnit.M, BufferUnit.K)
        jobInfo.usedResources.cores == 1
        jobInfo.usedResources.nodes == 1
        jobInfo.usedResources.walltime == Duration.parse("PT3M58S")
        jobInfo.usedResources.queue  == "compute"
        jobInfo.runTime == Duration.parse("PT3M58S")

        /** Directories and files */
        jobInfo.execHome == "/path/to/file"

        /** Timestamps */
        jobInfo.submitTime.toInstant().toEpochMilli() / 1000 == 1687222030
        jobInfo.eligibleTime.toInstant().toEpochMilli() / 1000 == 1687234198
        jobInfo.startTime.toInstant().toEpochMilli() / 1000 == 1687234198
        jobInfo.endTime.toInstant().toEpochMilli() / 1000 == 1687234436
    }

    def "test processExtendedOutputFromJson with sacct_requeued.json"() {
        when:
        GenericJobInfo jobInfo = jobManager.processExtendedOutputFromJson(getResourceFile("sacct_requeued.json").text)

        then:
        jobInfo.executionHosts == ["compute015"]
    }
}
