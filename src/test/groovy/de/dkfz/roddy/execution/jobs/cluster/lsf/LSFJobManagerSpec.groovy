/*
 * Copyright (c) 2019 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/BatchEuphoria/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.JobManagerImplementationBaseSpec
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import spock.lang.Ignore

import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

class LSFJobManagerSpec extends JobManagerImplementationBaseSpec<LSFJobManager> {

    //////////////////////////////////////
    // Time zone helpers and tests.
    //////////////////////////////////////

    @CompileStatic
    String zonedDateTimeToString(ZonedDateTime date) {
        DateTimeFormatter.ofPattern('MMM ppd HH:mm').withLocale(Locale.ENGLISH).format(date)
    }

    @CompileStatic
    String localDateTimeToLSFString(LocalDateTime date) {
        // Important here is, that LSF puts " L" or other status codes at the end of some dates, e.g. FINISH_DATE
        // Thus said, " L" does not apply for all dates reported by LSF!
        DateTimeFormatter.ofPattern('MMM ppd HH:mm').withLocale(Locale.ENGLISH).format(date) + " L"
    }

    void testZonedDateTimeToString(input, expected) {
        expect:
        zonedDateTimeToString(input) == expected

        where:
        input                                                              | expected
        ZonedDateTime.of(2000, 1, 1, 10, 21, 0, 0, ZoneId.systemDefault()) | "Jan  1 10:21"
        ZonedDateTime.of(2000, 5, 7, 10, 21, 0, 0, ZoneId.systemDefault()) | "May  7 10:21"
    }

    void testLocalDateTimeToLSFString(input, expected) {
        expect:
        localDateTimeToLSFString(input) == expected

        where:
        input                                | expected
        LocalDateTime.of(2000, 1, 1, 10, 21) | "Jan  1 10:21 L"
        LocalDateTime.of(2000, 5, 7, 10, 21) | "May  7 10:21 L"
    }

    //////////////////////////////////////
    // Class tests
    //////////////////////////////////////

    def "test parseTime"(String time, ZonedDateTime expected) {
        expect:
        createJobManager().parseTime(time) == expected

        where:
        time                  | expected
        "Jan  1 10:21"        | ZonedDateTime.of(LocalDateTime.now().year, 1, 1, 10, 21, 0, 0, ZoneId.systemDefault())
        "Jan  2 10:21 L"      | ZonedDateTime.of(LocalDateTime.now().year, 1, 2, 10, 21, 0, 0, ZoneId.systemDefault())
        "May  3 10:21 2016"   | ZonedDateTime.of(2016, 5, 3, 10, 21, 0, 0, ZoneId.systemDefault())
        "May  4 10:21 2016 L" | ZonedDateTime.of(2016, 5, 4, 10, 21, 0, 0, ZoneId.systemDefault())
    }

    def "test parseTime with malformatted string"(String time, expected) {
        when:
        createJobManager().parseTime(time)

        then:
        thrown(expected)

        where:
        time          | expected
        null          | DateTimeParseException
        ""            | DateTimeParseException
        "Hfjfj"       | DateTimeParseException
        "Mai 1 10:21" | DateTimeParseException
    }

    def "test stripAwayStatusInfo"() {
        expect:
        LSFJobManager.stripAwayStatusInfo(time) == expected

        where:
        time             | expected
        null             | null
        ""               | ""
        "Jan  1 10:21"   | "Jan  1 10:21"
        "Jan  1 10:21 1" | "Jan  1 10:21"
        "Jan  1 10:21 E" | "Jan  1 10:21"
        "Jan  1 10:21 F" | "Jan  1 10:21"
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
        given:
        def jm = createJobManagerWithModifiedExecutionService("queryJobStateByIdTest.json")

        when:
        def result = jm.queryJobInfoByID(testJobID)

        then:
        result.jobState == JobState.COMPLETED_SUCCESSFUL
    }

    def "test queryJobStatesByJob"() {
        return null
    }

    def "test queryJobStatesByID"() {
        given:
        def jm = createJobManagerWithModifiedExecutionService("queryJobStateByIdTest.json")


        when:
        def result = jm.queryJobInfoByID([testJobID])

        then:
        result.size() == 1
        result[testJobID].jobState == JobState.COMPLETED_SUCCESSFUL
    }

    def "test queryAllJobStates"() {
        return null
    }

    def "test queryExtendedJobStatesByJob"() {
        given:
        def jobManager = createJobManagerWithModifiedExecutionService("queryExtendedJobStateByIdTest.json")
        BEJob job = new BEJob(testJobID, jobManager)

        when:
        def result = jobManager.queryExtendedJobInfoByJob([job])

        then:
        result.size() == 1
        result[job]
    }

    def "test queryExtendedJobStatesById"() {

        given:
        def jobManager = createJobManagerWithModifiedExecutionService("queryExtendedJobStateByIdTest.json")

        ExtendedJobInfo expected = new ExtendedJobInfo(testJobID, JobState.COMPLETED_SUCCESSFUL)
        expected.requestedResources = new ResourceSet(null, null, null, null, Duration.ofMinutes(10), null, "short-dmg", null)
        expected.usedResources = new ResourceSet(null, new BufferValue(5452595, BufferUnit.k), null, 1, Duration.ofSeconds(1), null, "short-dmg", null)
        expected.jobName = "ls -l"
        expected.command = "/home/testuser/somescript.sh"
        expected.submissionHost = "from-host"
        expected.executionHosts = ["exec-host"]
        expected.user = "otptest"
        expected.rawResourceRequest = 'select[type == local] order[r15s:pg] '
        expected.parentJobIDs = ["22004"]
        expected.execHome = "/some/test"
        expected.processesInJob = ["46782", "46796", "46798", "46915", "47458", "47643"]
        expected.exitCode = 0
        expected.execCwd = "/some/test"
        expected.cwd = '$HOME'
        expected.projectName = "default"
        expected.cpuTime = Duration.ofSeconds(1)
        expected.runTime = Duration.ofSeconds(1)

        when:
        Map<BEJobID, ExtendedJobInfo> result = jobManager.queryExtendedJobInfoByID([testJobID])
        ExtendedJobInfo jobInfo = result[testJobID]

        then:
        result.size() == 1

        when:
        // Small hack to get the right year. LSF won't always report years in its various date fields.
        // This needs to be configured by the LSF cluster administrators.
        ZonedDateTime testTime = ZonedDateTime.of(jobInfo.submitTime.year, 12, 28, 19, 56, 0, 0, ZoneId.systemDefault())
        expected.submitTime = testTime
        expected.startTime = testTime
        expected.endTime = testTime

        then:
        jobInfo == expected
    }

    def "test convertBJobsResultLinesToResultMap"() {
        given:
        def jsonFile = getResourceFile(LSFJobManagerSpec,"convertBJobsResultLinesToResultMapTest.json")
        def json = jsonFile.text

        when:
        Map<BEJobID, Map<String, Object>> map = LSFJobManager.convertBJobsJsonOutputToResultMap(json)
        def jobId = map.keySet()[0]

        then:
        map.size() == 6
        jobId.id == "487641"
        map[jobId]["JOBID"] == "487641"
        map[jobId]["JOB_NAME"] == "RoddyTest_testScript"
        map[jobId]["STAT"] == "EXIT"
        map[jobId]["FINISH_TIME"] == "Jan  7 09:59 L"
    }

    /**
     * This test should not be run by default, as it runs quite a while (on purpose).
     * Reenable it, if you run into memory leaks.
     */
    @Ignore
    def "test massive ConvertBJobsResultLinesToResultMap"(def _entries, def value) {
        when:
        int entries = _entries[0]
        String template1 = getResourceFile("bjobsJobTemplatePart1.txt").text
        String template2 = getResourceFile("bjobsJobTemplatePart2.txt").text
        List<String> lines = new LinkedList<>()

        lines << "{"
        lines << '  "COMMAND":"bjobs",'
        lines << '  "JOBS":"' + entries + '",'
        lines << '  "RECORDS":['

        int maximum = 1000000 + entries - 1
        for (int i = 1000000; i <= maximum; i++) {
            lines += template1.readLines()
            lines << '      "JOBID":"' + i + '",'
            lines << '      "JOB_NAME":"r181217_003553288_Strand_T_150_aTestJob",'
            lines += template2.readLines()
            if (i < maximum)
                lines << "      ,"
        }
        lines << "  ]"
        lines << "}"
        println("Entries ${entries}")
        def result = LSFJobManager.convertBJobsJsonOutputToResultMap(lines.join("\n"))

        then:
        result.size() == entries

        where:
        _entries | value
        [1]      | true
        [10]     | true
        [100]    | true
        [1000]   | true
        [2000]   | true
        [4000]   | true
        [8000]   | true
        [16000]  | true
    }

    def "test convertJobDetailsMapToGenericJobInfoObject"() {
        given:
        def parms = JobManagerOptions.create().build()
        TestExecutionService testExecutionService = new TestExecutionService("test", "test")
        LSFJobManager jm = new LSFJobManager(testExecutionService, parms)

        Object parsedJson = new JsonSlurper().parseText(getResourceFile(LSFJobManagerSpec, resourceFile).text)
        List records = (List) parsedJson.getAt("RECORDS")

        when:
        ExtendedJobInfo jobInfo = jm.convertJobDetailsMapToGenericJobInfoObject(records.get(0))

        then:
        jobInfo != null
        jobInfo.jobID.toString() == "22005"
        jobInfo.command == expectedCommand

        where:
        resourceFile                                     | expectedJobId | expectedCommand
        "queryExtendedJobStateByIdTest.json"             | "22005"       | "/home/testuser/somescript.sh"
        "queryExtendedJobStateByIdWithoutListsTest.json" | "22005"       | "/home/testuser/somescript.sh"
        "queryExtendedJobStateByIdEmptyTest.json"        | "22005"       | null
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
