/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue

import java.time.*
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.regex.Matcher

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import spock.lang.*

class LSFJobManagerSpec extends Specification {

    static final File getResourceFile(String file) {
        new File("src/test/resources/de/dkfz/roddy/execution/jobs/cluster/lsf/", file)
    }

    void "Test convertJobDetailsMapToGenericJobInfoObject"() {
        given:
        def parms = JobManagerOptions.create().build()
        TestExecutionService testExecutionService = new TestExecutionService("test", "test")
        LSFJobManager jm = new LSFJobManager(testExecutionService, parms)

        Object parsedJson = new JsonSlurper().parseText(getResourceFile(resourceFile).text)
        List records = (List) parsedJson.getAt("RECORDS")

        when:
        GenericJobInfo jobInfo = jm.convertJobDetailsMapToGenericJobInfoObject(records.get(0))

        then:
        jobInfo != null
        jobInfo.jobID.toString() == "22005"
        jobInfo.tool == expectedCommand

        where:
        resourceFile                                     | expectedJobId | expectedCommand
        "queryExtendedJobStateByIdTest.json"             | "22005"       | new File("ls -l")
        "queryExtendedJobStateByIdWithoutListsTest.json" | "22005"       | new File("ls -l")
        "queryExtendedJobStateByIdEmptyTest.json"        | "22005"       | null
    }

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

    void "test DAY pattern"() {
        when:
        boolean result = (input ==~ LSFJobManager.DAY)

        then:
        result == matches

        where:
        input | matches
        "1"   | true
        "12"  | true
        ""    | false
        "A"   | false
        "1A"  | false
        "A1"  | false
    }

    void "test MONTH pattern"() {
        when:
        boolean result = (input ==~ LSFJobManager.MONTH)

        then:
        result == matches

        where:
        input   | matches
        "Apr"   | true
        "April" | false
        "A"     | false
        "4"     | false
        "04"    | false
    }

    void "test YEAR pattern"() {
        when:
        boolean result = (input ==~ LSFJobManager.YEAR)

        then:
        result == matches

        where:
        input  | matches
        "1234" | true
        "123"  | false
        "12"   | false
        "1"    | false
        ""     | false
    }

    void "test TIME pattern, succeeding parses"() {
        when:
        String input = [hour, minute, second].findAll().join(":")
        Matcher matcher = input =~ LSFJobManager.TIME

        then:
        matcher.matches()
        matcher.group("time") == input
        matcher.group("hour") == hour
        matcher.group("minute") == minute
        matcher.group("second") == second

        where:
        hour | minute | second
        "12" | "34"   | "56"
        "01" | "02"   | "03"
        "1"  | "2"    | "3"
        "1"  | "2"    | null
    }

    void "test TIME pattern, failing parses"() {
        when:
        String input = [hour, minute, second].findAll().join(separator)
        Matcher matcher = input =~ LSFJobManager.TIME

        then:
        !matcher.matches()

        where:
        hour | minute | second | separator
        " "  | " "    | " "    | ":"
        "1"  | "2"    | " "    | ":"
        "1"  | "2"    | "3"    | ","
    }

    @Unroll
    void "test TIMESTAMP_PATTERN pattern, succeeding parses (#month #day #time #year #suffix)"() {
        when:
        Closure<String> ifNotEmpty = { String input ->
            return input == "-" ? null : input
        }

        List<String> timeComponent = time.split(":") as List<String>
        String input = [month, day, time, year, suffix].findAll { it != "-" }.join(" ")
        Matcher matcher = input =~ LSFJobManager.TIMESTAMP_PATTERN

        then:
        matcher.matches()

        matcher.group("day") == day
        matcher.group("month") == month
        matcher.group("year") == ifNotEmpty(year)
        matcher.group("time") == ifNotEmpty(time)
        matcher.group("hour") == ifNotEmpty(timeComponent[0])
        matcher.group("minute") == ifNotEmpty(timeComponent[1])
        matcher.group("second") == ifNotEmpty(timeComponent[2])
        matcher.group("lsfSuffix") == ifNotEmpty(suffix)

        where:
        month | day  | time       | year   | suffix
        "Jan" | "01" | "12:34"    | "-"    | "-"
        "Feb" | "2"  | "12:34"    | "-"    | "-"
        "Mar" | "03" | "12:34"    | "-"    | "L"
        "Apr" | "04" | "12:34"    | "1000" | "-"
        "May" | "05" | "12:34"    | "1000" | "L"
        "Jun" | "6"  | "12:34:56" | "1000" | "L"
        "Jul" | "07" | "12:34:56" | "1000" | "L"
        "Aug" | "88" | "00:00:00" | "1000" | "-"
        "Sep" | "9"  | "1:2:3"    | "1000" | "-"
        "Oct" | "10" | "12:3:4"   | "1000" | "-"
        "Nov" | "11" | "12:34:56" | "1000" | "-"
        "Dec" | "12" | "12:34"    | "1000" | "-"
    }

    @Unroll
    void "test TIMESTAMP_PATTERN pattern, failing parses (#input)"() {
        when:
        Matcher matcher = input =~ LSFJobManager.TIMESTAMP_PATTERN

        then:
        !matcher.matches()

        where:
        input                  |_
        "Aug 42 :: 1000"       |_
        "Sep 2 :18:20 1000"    |_
        "Oct 2 14::20 1000"    |_
        "Nov 2 14:18: 1000"    |_
        "Dec 2 14:18:20 19"    |_
        "Tst 2 14:18:20 1000"  |_
    }

    void "test parseTime"() {
        given:
        def jsonFile = getResourceFile("queryExtendedJobStateByIdTest.json")

        JobManagerOptions parms = JobManagerOptions.create().build()
        BEExecutionService testExecutionService = [
                execute: { String s -> new ExecutionResult(true, 0, jsonFile.readLines(), null) }
        ] as BEExecutionService
        LSFJobManager manager = new LSFJobManager(testExecutionService, parms)

        when:
        ZonedDateTime refTime = ZonedDateTime.now()
        ZonedDateTime earlierTime = refTime.minusDays(1)
        ZonedDateTime laterTime = refTime.plusDays(1)
        ZonedDateTime laterLastYear = laterTime.minusYears(1)

        then:
        manager.parseTime(zonedDateTimeToString(earlierTime)).truncatedTo(ChronoUnit.MINUTES).
                equals(earlierTime.truncatedTo(ChronoUnit.MINUTES))
        manager.parseTime(zonedDateTimeToString(laterTime)).truncatedTo(ChronoUnit.MINUTES).
                equals(laterLastYear.truncatedTo(ChronoUnit.MINUTES))
    }

    @Unroll
    void "parseTime, parses all known formats (#month #day #hour:#minute:#second #year #suffix #expectedYear)"() {
        given:
        def jsonFile = getResourceFile("queryExtendedJobStateByIdTest.json")

        JobManagerOptions parms = JobManagerOptions.create().build()
        BEExecutionService testExecutionService = [
                execute: { String s -> new ExecutionResult(true, 0, jsonFile.readLines(), null) }
        ] as BEExecutionService
        LSFJobManager manager = new LSFJobManager(testExecutionService, parms)

        String time = [hour, minute, second].findAll { it != "" }.join(":")
        String timestamp = [month, day, time, year, suffix].findAll { it != "" }.join(" ")

        when:
        ZonedDateTime result = manager.parseTime(timestamp, referenceDate)
        LocalDateTime resultTime = result.toLocalDateTime()

        then:
        result.monthValue == LSFJobManager.MONTH_VALUE[month]
        result.dayOfMonth == Integer.parseInt(day)
        result.year == expectedYear
        resultTime.hour == Integer.parseInt(hour)
        resultTime.minute == Integer.parseInt(minute)
        resultTime.second == (second ? Integer.parseInt(second) : 0)

        where:
        month | day  | hour | minute | second | year   | suffix | expectedYear | referenceDate
        "Jan" | "01" | "01" | "02"   | ""     | ""     | ""     | 2021         | ZonedDateTime.of(2021, 1, 29, 19, 56, 0, 0, ZoneId.systemDefault())
        "Feb" | "02" | "01" | "02"   | ""     | ""     | "L"    | 2020         | ZonedDateTime.of(2021, 1, 29, 19, 56, 0, 0, ZoneId.systemDefault())
        "Feb" | "02" | "01" | "02"   | ""     | ""     | "L"    | 2021         | ZonedDateTime.of(2021, 2,  3, 19, 56, 0, 0, ZoneId.systemDefault())
        "Mar" | "03" | "01" | "02"   | ""     | "1000" | ""     | 1000         | ZonedDateTime.of(2021, 1, 29, 19, 56, 0, 0, ZoneId.systemDefault())
        "Apr" | "04" | "01" | "02"   | ""     | "1001" | "L"    | 1001         | ZonedDateTime.of(2021, 1, 29, 19, 56, 0, 0, ZoneId.systemDefault())
        "May" | "5"  | "01" | "02"   | "03"   | "1002" | ""     | 1002         | ZonedDateTime.of(2021, 1, 29, 19, 56, 0, 0, ZoneId.systemDefault())
        "Jun" | "6"  | "01" | "02"   | "03"   | "1003" | "L"    | 1003         | ZonedDateTime.of(2021, 1, 29, 19, 56, 0, 0, ZoneId.systemDefault())
    }

//    void "test queryExtendedJobStateById with overdue date"() {
//        given:
//        JobManagerOptions parms = JobManagerOptions.create().build()
//        def jsonFile = getResourceFile("queryExtendedJobStateByIdTest.json")
//        BEExecutionService testExecutionService = [
//                execute: { String s -> new ExecutionResult(true, 0, jsonFile.readLines(), null) }
//        ] as BEExecutionService
//        LSFJobManager manager = new LSFJobManager(testExecutionService, parms)
//
//        when:
//        Map<BEJobID, GenericJobInfo> result = manager.queryExtendedJobStateById([new BEJobID("22005")])
//
//        then:
//        result.size() == 0
//    }

    void "test queryExtendedJobStateById"() {
        given:
        JobManagerOptions parms = JobManagerOptions.create().setMaxTrackingTimeForFinishedJobs(Duration.ofDays(360000)).build()
        def jsonFile = getResourceFile("queryExtendedJobStateByIdTest.json")
        BEExecutionService testExecutionService = [
                execute: { String s -> new ExecutionResult(true, 0, jsonFile.readLines(), null) }
        ] as BEExecutionService
        LSFJobManager manager = new LSFJobManager(testExecutionService, parms)

        when:
        Map<BEJobID, GenericJobInfo> result = manager.queryExtendedJobStateById([new BEJobID("22005")])

        then:
        result.size() == 1
        GenericJobInfo jobInfo = result.get(new BEJobID("22005"))
        jobInfo
        jobInfo.askedResources.size == null
        jobInfo.askedResources.mem == null
        jobInfo.askedResources.cores == null
        jobInfo.askedResources.nodes == null
        jobInfo.askedResources.walltime == Duration.ofMinutes(10)
        jobInfo.askedResources.storage == null
        jobInfo.askedResources.queue == "short-dmg"
        jobInfo.askedResources.nthreads == null
        jobInfo.askedResources.swap == null

        jobInfo.usedResources.size == null
        jobInfo.usedResources.mem == new BufferValue(5452595, BufferUnit.k)
        jobInfo.usedResources.cores == null
        jobInfo.usedResources.nodes == 1
        jobInfo.usedResources.walltime == Duration.ofSeconds(1)
        jobInfo.usedResources.storage == null
        jobInfo.usedResources.queue == "short-dmg"
        jobInfo.usedResources.nthreads == null
        jobInfo.usedResources.swap == null

        jobInfo.jobName == "ls -l"
        jobInfo.tool == new File("ls -l")
        jobInfo.jobID == new BEJobID("22005")

        // The year-parsing/inference is checked in another test. Here just take the parsed value.
        ZonedDateTime testTime = ZonedDateTime.of(jobInfo.submitTime.year, 12, 28, 19, 56, 0, 0, ZoneId.systemDefault())
        jobInfo.submitTime == testTime
        jobInfo.eligibleTime == null
        jobInfo.startTime == testTime
        jobInfo.endTime == testTime
        jobInfo.executionHosts == ["exec-host", "exec-host"]
        jobInfo.submissionHost == "from-host"
        jobInfo.priority == null
        jobInfo.logFile == null
        jobInfo.errorLogFile == null
        jobInfo.inputFile == null
        jobInfo.user == "otptest"
        jobInfo.userGroup == null
        jobInfo.resourceReq == 'select[type == local] order[r15s:pg] '
        jobInfo.startCount == null
        jobInfo.account == null
        jobInfo.server == null
        jobInfo.umask == null
        jobInfo.parameters == null
        jobInfo.parentJobIDs == ["22004"]
        jobInfo.otherSettings == null
        jobInfo.jobState == JobState.COMPLETED_SUCCESSFUL
        jobInfo.userTime == null
        jobInfo.systemTime == null
        jobInfo.pendReason == null
        jobInfo.execHome == "/some/test"
        jobInfo.execUserName == null
        jobInfo.pidStr == ["46782", "46796", "46798", "46915", "47458", "47643"]
        jobInfo.pgidStr == null
        jobInfo.exitCode == 0
        jobInfo.jobGroup == null
        jobInfo.description == null
        jobInfo.execCwd == "/some/test"
        jobInfo.askedHostsStr == null
        jobInfo.cwd == '$HOME'
        jobInfo.projectName == "default"
        jobInfo.cpuTime == Duration.ofSeconds(1)
        jobInfo.runTime == Duration.ofSeconds(1)
        jobInfo.timeUserSuspState == null
        jobInfo.timePendState == null
        jobInfo.timePendSuspState == null
        jobInfo.timeSystemSuspState == null
        jobInfo.timeUnknownState == null
        jobInfo.timeOfCalculation == null
    }


    def "test convertBJobsResultLinesToResultMap"() {
        given:
        def jsonFile = getResourceFile("convertBJobsResultLinesToResultMapTest.json")
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

//    def "test filterJobMapByAge"() {
//        given:
//        def jsonFile = getResourceFile("convertBJobsResultLinesToResultMapTest.json")
//        def json = jsonFile.text
//
//        when:
//        LocalDateTime referenceDate = LocalDateTime.now()
//        int minutesToSubtract = 20
//        def records = LSFJobManager.convertBJobsJsonOutputToResultMap(json)
//        records.each {
//            def id, def record ->
//                def timeForRecord = LocalDateTime.of(referenceDate.year, referenceDate.month, referenceDate.dayOfMonth, referenceDate.hour, referenceDate.minute, referenceDate.second).minusMinutes(minutesToSubtract)
//                minutesToSubtract -= 4
//                record["FINISH_TIME"] = localDateTimeToLSFString(timeForRecord)
//        }
//        records = LSFJobManager.filterJobMapByAge(records, Duration.ofMinutes(10))
//        def id = records.keySet()[0]
//
//        then:
//        records.size() == 3
//        id.id == "491861"
//    }

    /**
     * This test should not be run by default, as it runs quite a while (on purpose).
     * Reenable it, if you run into memory leaks.
     */
    @Ignore
    def testMassiveConvertBJobsResultLinesToResultMap(def _entries, def value) {
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
}
