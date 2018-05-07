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
import de.dkfz.roddy.tools.*
import groovy.json.JsonSlurper

import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Factory for the management of LSF cluster systems.
 *
 *
 */
@groovy.transform.CompileStatic
class LSFJobManager extends AbstractLSFJobManager {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(LSFJobManager.class.getSimpleName())


    private static final String LSF_COMMAND_QUERY_STATES = "bjobs -a -o -hms -json \"jobid job_name stat user queue " +
            "job_description proj_name job_group job_priority pids exit_code from_host exec_host submit_time start_time " +
            "finish_time cpu_used run_time user_group swap max_mem runtimelimit sub_cwd " +
            "pend_reason exec_cwd output_file input_file effective_resreq exec_home slots error_file command dependency \""
    private static final String LSF_COMMAND_DELETE_JOBS = "bkill"

    private String getQueryCommand() {
        return LSF_COMMAND_QUERY_STATES
    }

    @Override
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates = [:]
        for (BEJobID id : jobIds) {
            Map<String, Object> jobDetails = runBjobs([id]).get(id)
            queriedExtendedStates.put(id, queryJobInfo(jobDetails))
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

    LSFJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    protected LSFCommand createCommand(BEJob job) {
        return new LSFCommand(this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id, job.tool?.getAbsolutePath() ?: job.getToolScript())
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String commandString) {
        return new LSFCommandParser(commandString).toGenericJobInfo();
    }

    protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs) {
        runBjobs(jobIDs).collectEntries { BEJobID jobID, Object value ->
            JobState js = parseJobState(value["STAT"] as String)
            if (logger.isVerbosityHigh())
                logger.postAlwaysInfo("   Extracted jobState: " + js.toString())
            [(jobID): js]
        } as Map<BEJobID, JobState>

    }

    private Map<BEJobID, Map<String, Object>> runBjobs(List<BEJobID> jobIDs) {
        StringBuilder queryCommand = new StringBuilder(getQueryCommand())

        // user argument must be passed before the job IDs
        if (isTrackingOfUserJobsEnabled)
            queryCommand << " -u $userIDForQueries "

        if (jobIDs && jobIDs.size() < 10) {
            queryCommand << " ${jobIDs*.id.join(" ")} "
        }

        ExecutionResult er = executionService.execute(queryCommand.toString())
        List<String> resultLines = er.resultLines

        Map<BEJobID, Map<String, Object>> result = [:]

        if (er.successful) {
            if (resultLines.size() >= 1) {
                String rawJson = resultLines.join("\n")
                Object parsedJson = new JsonSlurper().parseText(rawJson)
                List records = (List) parsedJson.getAt("RECORDS")
                records.each {
                    BEJobID jobID = new BEJobID(it["JOBID"] as String)
                    String jobState = it["STAT"]
                    if (logger.isVerbosityMedium())
                        logger.postAlwaysInfo(["Bjobs BEJob line: " + it,
                                               "	Entry in arr[ID]: " + jobID,
                                               "   Entry in arr[STAT]: " + jobState].join("\n"))
                    result.put(jobID, it as Map<String, Object>)
                }
            }

        } else {
            String error = "Job status couldn't be updated. \n command: ${queryCommand} \n status code: ${er.exitCode} \n result: ${er.resultLines}"
            logger.warning(error)
            throw new BEException(error)
        }
        return result
    }

    private static LocalDateTime parseTime(String str) {
        DateTimeFormatter datePattern = DateTimeFormatter.ofPattern("MMM ppd HH:mm yyyy").withLocale(Locale.ENGLISH)
        LocalDateTime date = LocalDateTime.parse(str + " " + LocalDateTime.now().getYear(), datePattern)
        if (date > LocalDateTime.now()) {
            return date.minusYears(1)
        }
        return date
    }

    /**
     * Used by @getJobDetails to set JobInfo
     */
    private GenericJobInfo queryJobInfo(Map<String, Object> jobResult) {

        GenericJobInfo jobInfo
        BEJobID jobID = new BEJobID(jobResult["JOBID"] as String)

        List<String> dependIDs = ((String) jobResult["DEPENDENCY"]).tokenize(/&/).collect { it.find(/\d+/) }
        jobInfo = new GenericJobInfo(jobResult["JOB_NAME"] as String, new File(jobResult["COMMAND"] as String), jobResult["JOBID"] as String, null, dependIDs)

        String queue = jobResult["QUEUE"] ?: null
        Duration runTime = catchAndLogExceptions {
            jobResult["RUN_TIME"] ? parseColonSeparatedHHMMSSDuration(jobResult["RUN_TIME"] as String) : null
        }
        BufferValue swap = catchAndLogExceptions {
            jobResult["SWAP"] ? new BufferValue((jobResult["SWAP"] as String).find("\\d+"), BufferUnit.m) : null
        }
        BufferValue memory = catchAndLogExceptions {
            String unit = (jobResult["MAX_MEM"] as String).find("[a-zA-Z]+")
            BufferUnit bufferUnit
            if (unit == "Gbytes")
                bufferUnit = BufferUnit.g
            else
                bufferUnit = BufferUnit.m
            jobResult["MAX_MEM"] ? new BufferValue((jobResult["MAX_MEM"] as String).find("([0-9]*[.])?[0-9]+"), bufferUnit) : null
        }
        Duration runLimit = catchAndLogExceptions {
            jobResult["RUNTIMELIMIT"] ? parseColonSeparatedHHMMSSDuration(jobResult["RUNTIMELIMIT"] as String) : null
        }
        Integer nodes = catchAndLogExceptions { jobResult["SLOTS"] ? jobResult["SLOTS"] as Integer : null }

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
        catchAndLogExceptions {
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

        if (jobResult["SUBMIT_TIME"])
            catchAndLogExceptions { jobInfo.setSubmitTime(parseTime(jobResult["SUBMIT_TIME"] as String)) }
        if (jobResult["START_TIME"])
            catchAndLogExceptions { jobInfo.setStartTime(parseTime(jobResult["START_TIME"] as String)) }
        if (jobResult["FINISH_TIME"])
            catchAndLogExceptions {
                jobInfo.setEndTime(parseTime((jobResult["FINISH_TIME"] as String).substring(0, (jobResult["FINISH_TIME"] as String).length() - 2)))
            }

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
