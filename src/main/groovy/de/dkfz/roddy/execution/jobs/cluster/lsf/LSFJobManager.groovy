/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
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
import groovy.transform.CompileStatic

import java.time.DateTimeException
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

    private static final String LSF_COMMAND_QUERY_STATES = "bjobs -a -hms -json -o \"jobid job_name stat finish_time\""
    private static final String LSF_COMMAND_QUERY_EXTENDED_STATES = "bjobs -a -hms -json -o \"jobid job_name stat user queue " +
            "job_description proj_name job_group job_priority pids exit_code from_host exec_host submit_time start_time " +
            "finish_time cpu_used run_time user_group swap max_mem runtimelimit sub_cwd " +
            "pend_reason exec_cwd output_file input_file effective_resreq exec_home slots error_file command dependency \""
    private static final String LSF_COMMAND_DELETE_JOBS = "bkill"

    static final DateTimeHelper dateTimeHelper = new DateTimeHelper("MMM ppd HH:mm yyyy", Locale.ENGLISH)

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
     * an exception if:
     *  - The string is empty OR
     *  - The string is wrong OR
     *  - The string is not in one of the four allowed date formats:
     *    * Short (with status flag)
     *    * Long (with status flag)
     */
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
        } else {
            throw new DateTimeException("The string ${str} is not a valid LSF datetime string and cannot be parsed.")
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
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates = [:]
        for (BEJobID id : jobIds) {
            Map<String, String> jobDetails = runBjobs([id], true)[id]
            if (jobDetails) // Ignore filtered / nonexistent ids
                queriedExtendedStates.put(id, convertJobDetailsMapToGenericJobInfoObject(jobDetails))
        }
        return queriedExtendedStates
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
    GenericJobInfo parseGenericJobInfo(String commandString) {
        return new LSFCommandParser(commandString).toGenericJobInfo();
    }

    protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs) {
        def bjobs = runBjobs(jobIDs, false)
        bjobs.collectEntries { BEJobID jobID, Object value ->
            JobState js = parseJobState(value["STAT"] as String)
            [(jobID): js]
        } as Map<BEJobID, JobState>

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

        Map<BEJobID, Map<String, String>> result = convertBJobsJsonOutputToResultMap(resultLines.join("\n"))
        return result; //filterJobMapByAge(result, maxTrackingTimeForFinishedJobs)
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
     * For all entries in records, check if they are finished and if so, check if they are younger (or older) than
     * the maximum age.
     * @param records A map of informational entries for one or more job ids
     * @param reference Time A timestamp which can be set. It is compared against the timestamp of finished entries.
     * @param maxJobKeepDuration Defines the maximum duration
     * @return The map of records where too old entries are filtered out.
     */
    @CompileStatic
    static Map<BEJobID, Map<String, String>> filterJobMapByAge(
            Map<BEJobID, Map<String, String>> records,
            Duration maxJobKeepDuration
    ) {
        records.findAll { def k, def record ->
            String finishTime = record["FINISH_TIME"]
            boolean youngEnough = true
            if (finishTime) {
                withCaughtAndLoggedException {
                    ZonedDateTime _finishTime = parseTime(stripAwayStatusInfo(finishTime))
                    Duration timeSpan = Duration.between(_finishTime.toLocalDateTime(), LocalDateTime.now())
                    if (dateTimeHelper.durationExceeds(timeSpan, maxJobKeepDuration))
                        youngEnough = false
                }
            }
            youngEnough
        }
    }

    /**
     * Used by @getJobDetails to set JobInfo
     */
    GenericJobInfo convertJobDetailsMapToGenericJobInfoObject(Map<String, String> _jobResult) {
        // Remove empty entries first to keep the output clean (use null, where the value is null or empty.)
        Map<String, String> jobResult = _jobResult.findAll { String k, String v -> v }

        GenericJobInfo jobInfo
        BEJobID jobID
        String JOBID = jobResult["JOBID"]
        try {
            jobID = new BEJobID(JOBID)
        } catch (Exception exp) {
            throw new BEException("Job ID '${JOBID}' could not be transformed to BEJobID ")
        }

        List<String> dependIDs = jobResult["DEPENDENCY"]?.tokenize(/&/)?.collect { it.find(/\d+/) }
        String _cmd = jobResult["COMMAND"];
        jobInfo = new GenericJobInfo(jobResult["JOB_NAME"], _cmd ? new File(_cmd) : null, jobID, null, dependIDs)

        /** Common */
        jobInfo.user = jobResult["USER"]
        jobInfo.userGroup = jobResult["USER_GROUP"]
        jobInfo.description = jobResult["JOB_DESCRIPTION"]
        jobInfo.projectName = jobResult["PROJ_NAME"]
        jobInfo.jobGroup = jobResult["JOB_GROUP"]
        jobInfo.priority = jobResult["JOB_PRIORITY"]
        jobInfo.pidStr = jobResult["PIDS"]?.split(",")?.toList()
        jobInfo.submissionHost = jobResult["FROM_HOST"]
        jobInfo.executionHosts = jobResult["EXEC_HOST"]?.split(":")?.toList()

        /** Resources */
        String queue = jobResult["QUEUE"]
        Duration runLimit = safelyParseColonSeparatedDuration(jobResult["RUNTIMELIMIT"])
        Duration runTime = safelyParseColonSeparatedDuration(jobResult["RUN_TIME"])
        BufferValue memory = safelyCastToBufferValue(jobResult["MAX_MEM"])
        BufferValue swap = withCaughtAndLoggedException {
            String SWAP = jobResult["SWAP"]
            SWAP ? new BufferValue(SWAP.find("\\d+"), BufferUnit.m) : null
        }
        Integer nodes = withCaughtAndLoggedException { jobResult["SLOTS"] as Integer }

        jobInfo.usedResources = new ResourceSet(memory, null, nodes, runTime, null, queue, null)
        jobInfo.askedResources = new ResourceSet(null, null, null, runLimit, null, queue, null)
        jobInfo.resourceReq = jobResult["EFFECTIVE_RESREQ"]
        jobInfo.runTime = runTime
        jobInfo.cpuTime = safelyParseColonSeparatedDuration(jobResult["CPU_USED"])

        /** Status info */
        jobInfo.jobState = parseJobState(jobResult["STAT"])
        jobInfo.exitCode = jobInfo.jobState == JobState.COMPLETED_SUCCESSFUL ? 0 : (jobResult["EXIT_CODE"] as Integer)
        jobInfo.pendReason = jobResult["PEND_REASON"]

        /** Directories and files */
        jobInfo.cwd = jobResult["SUB_CWD"]
        jobInfo.execCwd = jobResult["EXEC_CWD"]
        jobInfo.logFile = getBjobsFile(jobResult["OUTPUT_FILE"], jobID, "out")
        jobInfo.errorLogFile = getBjobsFile(jobResult["ERROR_FILE"], jobID, "err")
        jobInfo.inputFile = jobResult["INPUT_FILE"] ? new File(jobResult["INPUT_FILE"]) : null
        jobInfo.execHome = jobResult["EXEC_HOME"]

        /** Timestamps */
        jobInfo.submitTime = safelyParseTime(jobResult["SUBMIT_TIME"])
        jobInfo.startTime = safelyParseTime(jobResult["START_TIME"])
        jobInfo.endTime = safelyParseTime(stripAwayStatusInfo(jobResult["FINISH_TIME"]))

        return jobInfo
    }

    Duration safelyParseColonSeparatedDuration(String value) {
        withCaughtAndLoggedException {
            value ? parseColonSeparatedHHMMSSDuration(value) : null
        }
    }

    ZonedDateTime safelyParseTime(String time) {
        if (time)
            return withCaughtAndLoggedException {
                return parseTime(time)
            }
        return null
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
    String getQueryJobStatesCommand() {
        return null
    }

    @Override
    String getExtendedQueryJobStatesCommand() {
        return null
    }
}
