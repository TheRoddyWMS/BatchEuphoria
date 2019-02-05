/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs


import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.config.ResourceSetSize
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.ExtendedJobInfo
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.execution.jobs.cluster.JobManagerImplementationBaseSpec
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.TimeUnit

import java.time.Duration
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

class PBSJobManagerSpec extends JobManagerImplementationBaseSpec<PBSJobManager> {

    /**
     * convertResourceSet is implemented in ClusterJobManager but calls methods in the actual JobManager instance
     */
    void "test convertResourceSet"(String expected, Integer mem, Integer cores, Integer nodes, String walltime) {
        given:
        BufferValue _mem = mem ? new BufferValue(mem, BufferUnit.m) : null
        TimeUnit _walltime = walltime ? new TimeUnit(walltime) : null
        ResourceSet test = new ResourceSet(ResourceSetSize.l, _mem, cores, nodes, (TimeUnit) _walltime, null, null, null)

        expect:
        createJobManager().convertResourceSet(null, test).processingCommandString == expected

        where:
        expected                                                | mem  | cores | nodes | walltime
        "-l mem=1024M -l walltime=00:01:00:00 -l nodes=1:ppn=2" | 1024 | 2     | 1     | "h"
        "-l walltime=00:01:00:00 -l nodes=1:ppn=1"              | null | null  | 1     | "h"
        "-l mem=1024M -l nodes=1:ppn=2"                         | 1024 | 2     | null  | null
    }

    ExtendedJobInfo getGenericJobInfoObjectForTests() {
        ExtendedJobInfo expected = new ExtendedJobInfo(testJobID, JobState.QUEUED)
        expected.requestedResources = new ResourceSet(null, new BufferValue(37888, BufferUnit.M), 4, 1, Duration.ofHours(5), null, "debug", null)
        expected.usedResources = new ResourceSet(null, null, null, null, (Duration) null, null, "debug", null)
        expected.jobName = "ATestJob"
        expected.user = "testuser"
        expected.userGroup = ""
        expected.account = ""
        expected.umask = ""
        expected.submissionHost = "subm-host.example.com"
        expected.executionHosts = []
        expected.submitTime = ZonedDateTime.of(2018, 03, 28, 18, 39, 22, 0, ZoneOffset.systemDefault())
        expected.eligibleTime = ZonedDateTime.of(2018, 03, 28, 18, 39, 22, 0, ZoneOffset.systemDefault())
        expected.priority = "0"
        expected.logFile = new File("/somefolder/job.o22005.pbsserver")
        expected.errorLogFile = new File("/somefolder/job.o22005.pbsserver")
        expected.rawResourceRequest = '-v TOOL_ID=starAlignment,PARAMETER_FILE=/somefolder/exec/ATestJob.parameters,CONFIG_FILE=/somefolder/exec/ATestJob.parameters,debugWrapInScript=false,baseEnvironmentScript=/tbi/software/sourceme -N ATestJob -h -w /home/otp-data -j oe -o /somefolder/job.o$PBS_JOBID -l mem=37888M -l walltime=00:05:00:00 -l nodes=1:ppn=4 -q otp /somefolder/wrapInScript.sh'
        expected.server = "pbsserver"
        expected.parentJobIDs = []
        expected
    }

    def "test submitJob"() {
        return null
    }

    def "test addToListOfStartedJobs"() {
        return null
    }

    def "test startHeldJobs"() {
        return null
    }

    def "test killJobs"() {
        return null
    }

    def "test queryJobStateByJob"() {
        return null
    }

    def "test queryJobStateByID"() {
        return null
    }

    def "test queryJobInfByJob"() {
        return null
    }

    def "test queryJobInfByID"() {
        return null
    }

    def "test queryAllJobInf"() {
        return null
    }

    def "test queryExtendedJobInfByJob"() {
        return null
    }

    def "test queryExtendedJobInfByID"() {
        return null
    }

    void "test queryExtendedJobInfByID with queued job"() {

        given:

        def jobManager = createJobManagerWithModifiedExecutionService("processExtendedQstatXMLOutputWithQueuedJob.xml")

        ExtendedJobInfo expected = getGenericJobInfoObjectForTests()

        when:
        Map<BEJobID, ExtendedJobInfo> result = jobManager.queryExtendedJobInfo([testJobID])
        ExtendedJobInfo jobInfo = result.get(testJobID)

        then:
        result.size() == 1
        jobInfo.requestedResources == expected.requestedResources
        jobInfo.usedResources == expected.usedResources
        jobInfo.submitTime == expected.submitTime
        jobInfo == expected
    }

    void "test queryExtendedJobInfByID with finished job"() {

        given:

        def jobManager = createJobManagerWithModifiedExecutionService("processExtendedQstatXMLOutputWithFinishedJob.xml")

        ExtendedJobInfo expected = getGenericJobInfoObjectForTests()

        expected.usedResources = new ResourceSet(null, new BufferValue(6064, BufferUnit.k), null, null, Duration.ofSeconds(6), null, "otp", null)
        expected.startTime = ZonedDateTime.of(2018, 4, 5, 16, 39, 43, 0, ZoneId.systemDefault())
        expected.endTime = ZonedDateTime.of(2018, 4, 5, 16, 39, 54, 0, ZoneId.systemDefault())
        expected.executionHosts = ["exec-host"]
        expected.startCount = 1
        expected.parentJobIDs = ["6059780", "6059781"]
        expected.jobState = JobState.COMPLETED_UNKNOWN
        expected.exitCode = 0
        expected.cpuTime = Duration.ZERO
        expected.runTime = Duration.ofSeconds(10)

        when:
        Map<BEJobID, ExtendedJobInfo> result = jobManager.queryExtendedJobInfo([testJobID])
        ExtendedJobInfo jobInfo = result.get(testJobID)

        then:
        result.size() == 1
        jobInfo == expected
    }

    void "test queryExtendedJobInfoByID with empty XML output"() {
        given:
        def jm = createJobManagerWithModifiedExecutionService("queryExtendedJobInfoWithEmptyXML.xml")

        when:
        Map<BEJobID, ExtendedJobInfo> result = jm.queryExtendedJobInfo([testJobID])
        def jobInfo = result.get(testJobID)

        then:
        result.size() == 1
        jobInfo
    }

    void "test queryExtendedJobInfoByID, replace placeholder PBS_JOBID in logFile and errorLogFile with job id "() {
        given:
        def jm = createJobManagerWithModifiedExecutionService("queryExtendedJobInfoWithPlaceholderReplacement.xml")

        when:
        Map<BEJobID, ExtendedJobInfo> result = jm.queryExtendedJobInfo([testJobID])
        def jobInfo = result.get(testJobID)

        then:
        result.size() == 1
        jobInfo.logFile.absolutePath == "/logging_root_path/logfile.o22005.testServer"
        jobInfo.errorLogFile.absolutePath == "/logging_root_path/logfile.e22005.testServer"
    }

    def "test getEnvironmentVariableGlobs"() {
        return null
    }

    def "test getDefaultForHoldJobsEnabled"() {
        return null
    }

    def "test isHoldJobsEnabled"() {
        return null
    }

    def "test getUserEmail"() {
        return null
    }

    def "test getUserMask"() {
        return null
    }

    def "test getUserGroup"() {
        return null
    }

    def "test getUserAccount"() {
        return null
    }

    def "test executesWithoutJobSystem"() {
        return null
    }

    def "test convertResourceSet"() {
        return null
    }

    def "test collectJobIDsFromJobs"() {
        return null
    }

    def "test extractAndSetJobResultFromExecutionResult"() {
        return null
    }

    def "test createCommand"() {
        return null
    }

    def "test parseJobID"() {
        return null
    }

    def "test parseJobState"() {
        return null
    }

    def "test executeStartHeldJobs"() {
        return null
    }

    def "test executeKillJobs"() {
        return null
    }
}
