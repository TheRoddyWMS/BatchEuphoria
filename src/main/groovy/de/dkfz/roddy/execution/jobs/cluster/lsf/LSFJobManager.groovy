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
            "finish_time cpu_used run_time user_group swap max_mem runtimelimit sub_cwd " +
            "pend_reason exec_cwd output_file input_file effective_resreq exec_home slots error_file command dependency \""
    private static final String LSF_COMMAND_DELETE_JOBS = "bkill"

    static final DateTimeHelper dateTimeHelper = new DateTimeHelper("MMM ppd HH:mm yyyy", Locale.ENGLISH)

    LSFJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    static ZonedDateTime parseTime(String str) {
        ZonedDateTime date = dateTimeHelper.parseToZonedDateTime("${str} ${LocalDateTime.now().year}")
        /**
         * parseToZonedDateTime() parses a zoned time as provided by LSF. Unfortunately, LFS does not return the submission year!
         * The (kind of reasonable) assumption made here is that if the job's submission time (assuming the current
         * year) is later than the current time, then the job was submitted last year.
         */
        if (date > ZonedDateTime.now()) {
            return date.minusYears(1)
        }
        return date
    }

    @Override
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates = [:]
        for (BEJobID id : jobIds) {
            Map<String, Object> jobDetails = runBjobs([id], true).get(id)
            queriedExtendedStates.put(id, convertJobDetailsMapToGenericJobInfoobject(jobDetails))
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

    Map<BEJobID, Map<String, Object>> runBjobs(List<BEJobID> jobIDs, boolean extended) {
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

        Map<BEJobID, Map<String, Object>> result = convertBJobsJsonOutputToResultMap(resultLines.join("\n"))
        return filterJobMapByAge(result, maxAgeOfJobsForQueries)
    }

    static Map<BEJobID, Map<String, Object>> convertBJobsJsonOutputToResultMap(String rawJson) {
        Map<BEJobID, Map<String, Object>> result = [:]

        if (!rawJson)
            return result

        Object parsedJson = new JsonSlurper().parseText(rawJson)
        List records = (List) parsedJson["RECORDS"]
        for (record in records) {
            BEJobID jobID = new BEJobID(record["JOBID"] as String)
            result[jobID] = record as Map<String, Object>
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
    static Map<BEJobID, Map<String, Object>> filterJobMapByAge(
            Map<BEJobID, Map<String, Object>> records,
            Duration maxJobKeepDuration
    ) {
        records.findAll { def k, def record ->
            String finishTime = record["FINISH_TIME"]
            boolean youngEnough = true
            if (finishTime) {
                String timeString = "${finishTime[0..-3]}"
                withCaughtAndLoggedException {
                    ZonedDateTime _finishTime = parseTime(timeString)
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
    GenericJobInfo convertJobDetailsMapToGenericJobInfoobject(Map<String, Object> jobResult) {

        GenericJobInfo jobInfo
        BEJobID jobID
        try {
            jobID = new BEJobID(jobResult["JOBID"] as String)
        } catch (Exception exp) {
            throw new BEException("Job ID '${jobResult["JOBID"]}' could not be transformed to BEJobID ")
        }

        List<String> dependIDs = ((String) jobResult["DEPENDENCY"]) ? ((String) jobResult["DEPENDENCY"]).tokenize(/&/).collect { it.find(/\d+/) } : null
        jobInfo = new GenericJobInfo(jobResult["JOB_NAME"] as String ?: null, jobResult["COMMAND"] as String ? new File(jobResult["COMMAND"] as String) : null, jobID, null, dependIDs)

        String queue = jobResult["QUEUE"] ?: null
        Duration runTime = withCaughtAndLoggedException {
            jobResult["RUN_TIME"] ? parseColonSeparatedHHMMSSDuration(jobResult["RUN_TIME"] as String) : null
        }
        BufferValue swap = withCaughtAndLoggedException {
            jobResult["SWAP"] ? new BufferValue((jobResult["SWAP"] as String).find("\\d+"), BufferUnit.m) : null
        }
        BufferValue memory = withCaughtAndLoggedException {
            String unit = (jobResult["MAX_MEM"] as String).find("[a-zA-Z]+")
            BufferUnit bufferUnit
            if (unit == "Gbytes")
                bufferUnit = BufferUnit.g
            else
                bufferUnit = BufferUnit.m
            jobResult["MAX_MEM"] ? new BufferValue((jobResult["MAX_MEM"] as String).find("([0-9]*[.])?[0-9]+"), bufferUnit) : null
        }
        Duration runLimit = withCaughtAndLoggedException {
            jobResult["RUNTIMELIMIT"] ? parseColonSeparatedHHMMSSDuration(jobResult["RUNTIMELIMIT"] as String) : null
        }
        Integer nodes = withCaughtAndLoggedException { jobResult["SLOTS"] ? jobResult["SLOTS"] as Integer : null }

        ResourceSet usedResources = new ResourceSet(memory, null, nodes, runTime, null, queue, null)
        jobInfo.setUsedResources(usedResources)

        ResourceSet askedResources = new ResourceSet(null, null, null, runLimit, null, queue, null)
        jobInfo.setAskedResources(askedResources)

        jobInfo.setUser(jobResult["USER"] as String ?: null)
        jobInfo.setDescription(jobResult["JOB_DESCRIPTION"] as String ?: null)
        jobInfo.setProjectName(jobResult["PROJ_NAME"] as String ?: null)
        jobInfo.setJobGroup(jobResult["JOB_GROUP"] as String ?: null)
        jobInfo.setPriority(jobResult["JOB_PRIORITY"] as String ?: null)
        jobInfo.setPidStr(jobResult["PIDS"] as String ? (jobResult["PIDS"] as String).split(",").toList() : null)
        jobInfo.setJobState(parseJobState(jobResult["STAT"] as String))
        jobInfo.setExitCode(jobInfo.jobState == JobState.COMPLETED_SUCCESSFUL ? 0 : (jobResult["EXIT_CODE"] ? Integer.valueOf(jobResult["EXIT_CODE"] as String) : null))
        jobInfo.setSubmissionHost(jobResult["FROM_HOST"] as String ?: null)
        jobInfo.setExecutionHosts(jobResult["EXEC_HOST"] as String ? (jobResult["EXEC_HOST"] as String).split(":").toList() : null)
        withCaughtAndLoggedException {
            jobInfo.setCpuTime(jobResult["CPU_USED"] ? parseColonSeparatedHHMMSSDuration(jobResult["CPU_USED"] as String) : null)
        }
        jobInfo.setRunTime(runTime)
        jobInfo.setUserGroup(jobResult["USER_GROUP"] as String ?: null)
        jobInfo.setCwd(jobResult["SUB_CWD"] as String ?: null)
        jobInfo.setPendReason(jobResult["PEND_REASON"] as String ?: null)
        jobInfo.setExecCwd(jobResult["EXEC_CWD"] as String ?: null)
        jobInfo.setLogFile(getBjobsFile(jobResult["OUTPUT_FILE"] as String, jobID, "out"))
        jobInfo.setErrorLogFile(getBjobsFile(jobResult["ERROR_FILE"] as String, jobID, "err"))
        jobInfo.setInputFile(jobResult["INPUT_FILE"] ? new File(jobResult["INPUT_FILE"] as String) : null)
        jobInfo.setResourceReq(jobResult["EFFECTIVE_RESREQ"] as String ?: null)
        jobInfo.setExecHome(jobResult["EXEC_HOME"] as String ?: null)

        String submissionTime = jobResult["SUBMIT_TIME"]
        String startTime = jobResult["START_TIME"]
        String finishTime = jobResult["FINISH_TIME"]

        if (submissionTime)
            withCaughtAndLoggedException { jobInfo.setSubmitTime(parseTime(submissionTime)) }
        if (startTime)
            withCaughtAndLoggedException { jobInfo.setStartTime(parseTime(startTime as String)) }
        if (finishTime)
            withCaughtAndLoggedException { jobInfo.setEndTime(parseTime(finishTime[0..-3])) }

        return jobInfo
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
