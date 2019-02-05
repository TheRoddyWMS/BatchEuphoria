/*
 * Copyright (c) 2019 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/BatchEuphoria/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.tools.BashUtils
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.DateTimeHelper
import groovy.json.JsonSlurper

import java.time.Duration
import java.time.LocalDateTime
import java.time.ZonedDateTime

/**
 * Factory for the management of LSF cluster systems.
 *
 *
 */
@groovy.transform.CompileStatic
class LSFJobManager extends AbstractLSFJobManager {

    private static final String LSF_COMMAND_QUERY_STATES = "bjobs -a -o -hms -json \"jobid job_name stat finish_time\""
    private static final String LSF_COMMAND_QUERY_EXTENDED_STATES = "bjobs -a -o -hms -json \"jobid job_name stat user queue " +
            "job_description proj_name job_group job_priority pids exit_code from_host exec_host submit_time start_time " +
            "finish_time cpu_used run_time user_group swap max_mem max_req_proc" +
            "memlimit runtimelimit swaplimit sub_cwd " +
            "pend_reason exec_cwd output_file input_file effective_resreq exec_home slots error_file command dependency \""
    private static final String LSF_COMMAND_DELETE_JOBS = "bkill"

    public static final String longReportedDateFormatWithoutStatus = "MMM ppd HH:mm yyyy"

    static final DateTimeHelper dateTimeHelper = new DateTimeHelper(longReportedDateFormatWithoutStatus, Locale.ENGLISH)

    LSFJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    /**
     * Time formats in LSF can be configured.
     * E.g. to our knowledge, by default, reported values look like:
     *   "Jan  1 10:21 L" with status information or "Jan  1 10:21" without them.
     * LSF can be configured to also report the year, so values would be reported as:
     *   "Jan  1 10:21 2010 L" with status information or "Jan  1 10:21 2010" without them.
     *
     * There might be different configurations, but we stick to those 4 versions.
     *
     * Furthermore, we do not know, if LSF will report in other languages than english.
     * We assume, that english is used for month names.
     *
     * We also assume, that the method will not be misused. If its misused, it will throw
     * an exception.
     */
    @Override
    ZonedDateTime parseTime(String str) {
        // Prevent NullPointerException, will throw a DateTimeParserException later
        if (str == null) str = ""
        String dateForParser = str
        if (str.size() == "Jan 01 01:00".size()) {
            // Lets start and see, if the date is reported in its short version, if so, add the year.
            dateForParser = "${str} ${LocalDateTime.now().year}"
        } else if (str.size() == "Jan 01 01:00 L".size()) {
            // Here we need to strip away the status first, then append the current year.
            dateForParser = "${stripAwayStatusInfo(str)} ${LocalDateTime.now().year}"
        } else if (str.size() == "Jan 01 01:00 1000".size()) {
            // Easy enough, just keep it like it is.
            dateForParser = str
        } else if (str.size() == "Jan 01 01:00 1000 L".size()) {
            // Again, strip away the status info.
            dateForParser = stripAwayStatusInfo(str)
        }

        // Finally we try to parse the date. Lets see if it works. If not, an exception is thrown.
        ZonedDateTime date = dateTimeHelper.parseToZonedDateTime(dateForParser)

        // If LSF is not configured to report the date, the (kind of reasonable) assumption made here is that if
        // the job's submission time (assuming the current year) is later than the current time, then the job was
        // submitted last year.

        if (date > ZonedDateTime.now()) {
            return date.minusYears(1)
        }
        return date
    }

    /**
     * Important here is, that LSF puts " L" or other status codes at the end of some dates, e.g. FINISH_DATE
     * Thus said, " L" does not apply for all dates reported by LSF! This method just removes the last two characters
     * of the time string.
     */
    static String stripAwayStatusInfo(String time) {
        String result = time
        if (time && time.size() > 2) {
            if (time[-2..-1] ==~ ~/[ ][a-zA-Z0-9]/)
                result = time[0..-3]
        }
        return result
    }

    @Override
    protected JobState parseJobState(String stateString) {
        JobState js = JobState.UNKNOWN

        if (stateString == "RUN")
            js = JobState.RUNNING
        if (stateString == "PSUSP")
            js = JobState.SUSPENDED
        if (stateString == "PEND")
            js = JobState.QUEUED
        if (stateString == "DONE")
            js = JobState.COMPLETED_SUCCESSFUL
        if (stateString == "EXIT")
            js = JobState.FAILED

        return js
    }

    protected LSFCommand createCommand(BEJob job) {
        return new LSFCommand(this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id, job.tool?.getAbsolutePath() ?: job.getToolScript())
    }

    @Override
    ExtendedJobInfo parseGenericJobInfo(String commandString) {
        return new LSFCommandParser(commandString).toGenericJobInfo();
    }

    @Override
    Map<BEJobID, JobInfo> queryJobInfo(List<BEJobID> jobIDs) {
        def bjobs = runBjobs(jobIDs, false)
        bjobs = bjobs.findAll { BEJobID k, Map<String, String> v -> v }

        return bjobs.collectEntries { BEJobID jobID, Object value ->
            JobState js = parseJobState(value["STAT"] as String)
            JobInfo jobInfo = new JobInfo(jobID)
            jobInfo.jobState = js
            [(jobID): jobInfo]
        } as Map<BEJobID, JobInfo>

    }

    @Override
    Map<BEJobID, ExtendedJobInfo> queryExtendedJobInfo(List<BEJobID> jobIds) {
        Map<BEJobID, ExtendedJobInfo> queriedExtendedStates = [:]
        for (BEJobID id : jobIds) {
            Map<String, String> jobDetails = runBjobs([id], true)[id]
            queriedExtendedStates.put(id, convertJobDetailsMapToGenericJobInfoObject(jobDetails))
        }
        return queriedExtendedStates
    }

    Map<BEJobID, Map<String, String>> runBjobs(List<BEJobID> jobIDs, boolean extended) {
        StringBuilder queryCommand = new StringBuilder(extended ? LSF_COMMAND_QUERY_EXTENDED_STATES : LSF_COMMAND_QUERY_STATES)

        // user argument must be passed before the job IDs
        if (isTrackingOfUserJobsEnabled)
            queryCommand << " -u $userIDForQueries "

        if (jobIDs && jobIDs.size() < 10) {
            queryCommand << " ${jobIDs*.id.join(" ")} "
        }

        ExecutionResult er = executionService.execute(queryCommand.toString())
        List<String> resultLines = er.resultLines

        if (!er.successful) {
            String error = "Job status couldn't be updated. \n command: ${queryCommand} \n status code: ${er.exitCode} \n result: ${er.resultLines}"
            throw new BEException(error)
        }

        return convertBJobsJsonOutputToResultMap(resultLines.join("\n"))
    }

    static Map<BEJobID, Map<String, String>> convertBJobsJsonOutputToResultMap(String rawJson) {
        Map<BEJobID, Map<String, String>> result = [:]

        if (!rawJson)
            return result

        Object parsedJson = new JsonSlurper().parseText(rawJson)
        List records = (List) parsedJson["RECORDS"]
        for (record in records) {
            BEJobID jobID = new BEJobID(record["JOBID"] as String)
            result[jobID] = record as Map<String, String>
        }

        result
    }

    /**
     * Used by @getJobDetails to set JobInfo
     */
    ExtendedJobInfo convertJobDetailsMapToGenericJobInfoObject(Map<String, String> _rawExtendedStates) {
        // Remove empty entries first to keep the output clean (use null, where the value is null or empty.)
        Map<String, String> extendedStates = _rawExtendedStates.findAll { String k, String v -> v }

        BEJobID jobID = toJobID(extendedStates["JOBID"])

        List<String> dependIDs = extendedStates["DEPENDENCY"]?.tokenize(/&/)?.collect { it.find(/\d+/) }
        ExtendedJobInfo jobInfo = new ExtendedJobInfo(extendedStates["JOB_NAME"], extendedStates["COMMAND"], jobID, null, dependIDs)

        /** Common */
        jobInfo.user = extendedStates["USER"]
        jobInfo.userGroup = extendedStates["USER_GROUP"]
        jobInfo.description = extendedStates["JOB_DESCRIPTION"]
        jobInfo.projectName = extendedStates["PROJ_NAME"]
        jobInfo.jobGroup = extendedStates["JOB_GROUP"]
        jobInfo.priority = extendedStates["JOB_PRIORITY"]
        jobInfo.processesInJob = extendedStates["PIDS"]?.split(",")?.toList()
        jobInfo.submissionHost = extendedStates["FROM_HOST"]

        /** Resources */
        jobInfo.executionHosts = extendedStates["EXEC_HOST"]?.split(":")?.toList()
        // Count hosts! The node count has no custom entry. However, we can calculate it from the host list.
        Integer noOfExecutionHosts = jobInfo.executionHosts?.sort()?.unique()?.size()
        String queue = extendedStates["QUEUE"]
        Duration requestedWalltime = safelyParseColonSeparatedDuration(extendedStates["RUNTIMELIMIT"])
        Duration usedWalltime = safelyParseColonSeparatedDuration(extendedStates["RUN_TIME"])
        BufferValue usedMemory = safelyCastToBufferValue(extendedStates["MAX_MEM"])
        BufferValue requestedMemory = safelyCastToBufferValue(extendedStates["MEMLIMIT"])
        Integer requestedCores = extendedStates["MAX_REQ_PROC"] as Integer
        BufferValue swap = withCaughtAndLoggedException {
            String SWAP = extendedStates["SWAP"]
            SWAP ? new BufferValue(SWAP.find("\\d+"), BufferUnit.m) : null
        }

        jobInfo.usedResources = new ResourceSet(usedMemory, null, noOfExecutionHosts, usedWalltime, null, queue, null)
        jobInfo.requestedResources = new ResourceSet(requestedMemory, requestedCores, null, requestedWalltime, null, queue, null)
        jobInfo.rawResourceRequest = extendedStates["EFFECTIVE_RESREQ"]
        jobInfo.runTime = usedWalltime
        jobInfo.cpuTime = safelyParseColonSeparatedDuration(extendedStates["CPU_USED"])

        /** Status info */
        jobInfo.jobState = parseJobState(extendedStates["STAT"])
        jobInfo.exitCode = jobInfo.jobState == JobState.COMPLETED_SUCCESSFUL ? 0 : (extendedStates["EXIT_CODE"] as Integer)
        jobInfo.pendReason = extendedStates["PEND_REASON"]

        /** Directories and files */
        jobInfo.cwd = extendedStates["SUB_CWD"]
        jobInfo.execCwd = extendedStates["EXEC_CWD"]
        jobInfo.logFile = getBjobsFile(extendedStates["OUTPUT_FILE"], jobID, "out")
        jobInfo.errorLogFile = getBjobsFile(extendedStates["ERROR_FILE"], jobID, "err")
        jobInfo.inputFile = extendedStates["INPUT_FILE"] ? new File(extendedStates["INPUT_FILE"]) : (File)null
        jobInfo.execHome = extendedStates["EXEC_HOME"]

        /** Timestamps */
        jobInfo.submitTime = safelyParseTime(extendedStates["SUBMIT_TIME"])
        jobInfo.startTime = safelyParseTime(extendedStates["START_TIME"])
        jobInfo.endTime = safelyParseTime(extendedStates["FINISH_TIME"])

        return jobInfo
    }

    BufferValue safelyCastToBufferValue(String MAX_MEM) {
        withCaughtAndLoggedException {
            if (MAX_MEM) {
                String bufferSize = MAX_MEM.find("([0-9]*[.])?[0-9]+")
                String unit = MAX_MEM.find("[a-zA-Z]+")
                BufferUnit bufferUnit = unit == "Gbytes" ? BufferUnit.g : BufferUnit.m
                return new BufferValue(bufferSize, bufferUnit)
            }
            return null
        }
    }

    private File getBjobsFile(String s, BEJobID jobID, String type) {
        if (!s) {
            return null
        } else if (executionService.execute("stat -c %F ${BashUtils.strongQuote(s)}").firstLine == "directory") {
            return new File(s, "${jobID.getId()}.${type}")
        } else {
            return new File(s)
        }
    }

    @Override
    protected ExecutionResult executeKillJobs(List<BEJobID> jobIDs) {
        String command = "${LSF_COMMAND_DELETE_JOBS} ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    @Override
    protected ExecutionResult executeStartHeldJobs(List<BEJobID> jobIDs) {
        String command = "bresume ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    @Override
    String parseJobID(String commandOutput) {
        String result = commandOutput.find(/<[0-9]+>/)
        //ToDo 'Group <resUsers>: Pending job threshold reached. Retrying in 60 seconds...'
        if (result == null)
            throw new BEException("Could not parse raw ID from: '${commandOutput}'")
        String exID = result.substring(1, result.length() - 1)
        return exID
    }

    @Override
    String getSubmissionCommand() {
        return "bsub"
    }

    @Override
    String getQueryCommandForJobInfo() {
        return null
    }

    @Override
    String getQueryCommandForExtendedJobInfo() {
        return null
    }
}
