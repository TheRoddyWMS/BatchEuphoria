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
import groovy.json.JsonOutput
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


    private static final String BJOBS_DELIMITER = "\t"
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
            Object jobDetails = runBjobs([id]).get(id)
            queriedExtendedStates.put(id, queryJobInfo(jobDetails))
        }
        return queriedExtendedStates
    }

    private JobState parseJobState(String stateString) {
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

    private Map<BEJobID, Object> runBjobs(List<BEJobID> jobIDs) {
        StringBuilder queryCommand = new StringBuilder(getQueryCommand())

        // user argument must be passed before the job IDs
        if (isTrackingOfUserJobsEnabled)
            queryCommand << " -u $userIDForQueries "

        if (jobIDs && jobIDs.size() < 10) {
            queryCommand << " ${jobIDs*.id.join(" ")} "
        }

        ExecutionResult er = executionService.execute(queryCommand.toString())
        List<String> resultLines = er.resultLines

        Map<BEJobID, Object> result = [:]

        if (er.successful) {
            if (resultLines.size() >= 1) {
                String rawJson = resultLines.join("\n")
                Object parsedJson
                try{
                    parsedJson = new JsonSlurper().parseText(rawJson)
                }catch (Exception exp){
                    logger.postAlwaysInfo("Extended job states couldn't be processed. Result lines: ${rawJson} Exception: ${exp}")
                    return [:]
                }
                List records = (List) parsedJson.getAt("RECORDS")
                records.each {
                    BEJobID jobID = new BEJobID(it["JOBID"] as String)
                    String jobState = it["STAT"]
                    logger.info(["QStat BEJob line: " + it,
                                 "	Entry in arr[ID]: " + jobID,
                                 "   Entry in arr[STAT]: " + jobState].join("\n"))
                    result.put(jobID, it)
                }

/*
                for (String line : resultLines) {
                    line = line.trim()
                    if (line.length() == 0) continue
                    if (!RoddyConversionHelperMethods.isInteger(line.substring(0, 1)))
                        continue //Filter out lines which have been missed which do not start with a number.

                    Object parsedJson = new JsonSlurper().parseText(line)
                    List records = (List) parsedJson.getAt("RECORDS")
                    Object jobResult = records.get(0)
                    BEJobID jobID = new BEJobID(jobResult["JOBID"] as String)

                    String[] split = line.split(BJOBS_DELIMITER)
                    final int ID = getPositionOfJobID()
                    final int JOBSTATE = getPositionOfJobState()
                    logger.info(["QStat BEJob line: " + line,
                                 "	Entry in arr[" + ID + "]: " + split[ID],
                                 "    Entry in arr[" + JOBSTATE + "]: " + split[JOBSTATE]].join("\n"))
                    TODO result.put(new BEJobID(split[ID]), new JsonSlurper().parseText(line))
                }*/
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
/*    private GenericJobInfo queryJobInfo(BEJobID jobID) {
        GenericJobInfo jobInfo

        List<String> dependIDs = jobDetails[32].tokenize(/&/).collect { it.find(/\d+/) }
        jobInfo = new GenericJobInfo(jobDetails[1], new File(jobDetails[31]), jobDetails[0], null, dependIDs)

        String[] jobResult = jobDetails.collect { String property -> if (property.trim() == "-") return "" else property }

        String queue = jobResult[4] ?: null
        Duration runTime = catchExceptionAndLog { jobResult[17] ? Duration.ofSeconds(Math.round(Double.parseDouble(jobResult[17].find("\\d+")))) : null }
        BufferValue swap = catchExceptionAndLog { jobResult[19] ? new BufferValue(jobResult[19].find("\\d+"), BufferUnit.m) : null }
        BufferValue memory = catchExceptionAndLog { jobResult[20] ? new BufferValue(jobResult[20].find("\\d+"), BufferUnit.m) : null }
        Duration runLimit = catchExceptionAndLog { jobResult[21] ? Duration.ofSeconds(Math.round(Double.parseDouble(jobResult[21].find("\\d+")))) : null }
        Integer nodes = catchExceptionAndLog { jobResult[29] ? jobResult[29] as Integer : null }

        ResourceSet usedResources = new ResourceSet(memory, null, nodes, runTime, null, queue, null)
        jobInfo.setUsedResources(usedResources)

        ResourceSet askedResources = new ResourceSet(null, null, null, runLimit, null, queue, null)
        jobInfo.setAskedResources(askedResources)

        jobInfo.setUser(jobResult[3] ?: null)
        jobInfo.setDescription(jobResult[5] ?: null)
        jobInfo.setProjectName(jobResult[6] ?: null)
        jobInfo.setJobGroup(jobResult[7] ?: null)
        jobInfo.setPriority(jobResult[8] ?: null)
        jobInfo.setPidStr(jobResult[9] ?: null)
        jobInfo.setJobState(parseJobState(jobResult[2]))
        jobInfo.setExitCode(jobInfo.jobState == JobState.COMPLETED_SUCCESSFUL ? 0 : (jobResult[10] ? Integer.valueOf(jobResult[10]) : null))
        jobInfo.setSubmissionHost(jobResult[11] ?: null)
        jobInfo.setExecutionHosts(jobResult[12] ?: null)
        catchExceptionAndLog { jobInfo.setCpuTime(jobResult[16] ? parseColonSeparatedHHMMSSDuration(jobResult[16]) : null) }
        jobInfo.setRunTime(runTime)
        jobInfo.setUserGroup(jobResult[18] ?: null)
        jobInfo.setCwd(jobResult[22] ?: null)
        jobInfo.setPendReason(jobResult[23] ?: null)
        jobInfo.setExecCwd(jobResult[24] ?: null)
        jobInfo.setLogFile(getBjobsFile(jobResult[25], jobID, "out"))
        jobInfo.setErrorLogFile(getBjobsFile(jobResult[30], jobID, "err"))
        jobInfo.setInputFile(jobResult[26] ? new File(jobResult[26]) : null)
        jobInfo.setResourceReq(jobResult[27] ?: null)
        jobInfo.setExecHome(jobResult[28] ?: null)

        if (jobResult[13])
            catchExceptionAndLog { jobInfo.setSubmitTime(parseTime(jobResult[13])) }
        if (jobResult[14])
            catchExceptionAndLog { jobInfo.setStartTime(parseTime(jobResult[14])) }
        if (jobResult[15])
            catchExceptionAndLog { jobInfo.setEndTime(parseTime(jobResult[15].substring(0, jobResult[15].length() - 2))) }

        return jobInfo
    }
*/

    /**
     * Used by @getJobDetails to set JobInfo
     */
    private GenericJobInfo queryJobInfo(Object jobResult) {

        GenericJobInfo jobInfo
        BEJobID jobID = new BEJobID(jobResult["JOBID"] as String)

        List<String> dependIDs = ((String) jobResult["DEPENDENCY"]).tokenize(/&/).collect { it.find(/\d+/) }
        jobInfo = new GenericJobInfo(jobResult["JOB_NAME"] as String, new File(jobResult["COMMAND"] as String), jobResult["JOBID"] as String, null, dependIDs)

        String queue = jobResult["QUEUE"] ?: null
        Duration runTime = catchExceptionAndLog {
            jobResult["RUN_TIME"] ? parseColonSeparatedHHMMSSDuration(jobResult["RUN_TIME"] as String) : null
        }
        BufferValue swap = catchExceptionAndLog {
            jobResult["SWAP"] ? new BufferValue((jobResult["SWAP"] as String).find("\\d+"), BufferUnit.m) : null
        }
        BufferValue memory = catchExceptionAndLog {
            jobResult["MAX_MEM"] ? new BufferValue((jobResult["MAX_MEM"] as String).find("\\d+"), BufferUnit.m) : null
        }
        Duration runLimit = catchExceptionAndLog {
            jobResult["RUNTIMELIMIT"] ? parseColonSeparatedHHMMSSDuration(jobResult["RUNTIMELIMIT"] as String) : null
        }
        Integer nodes = catchExceptionAndLog { jobResult["SLOTS"] ? jobResult["SLOTS"] as Integer : null }

        ResourceSet usedResources = new ResourceSet(memory, null, nodes, runTime, null, queue, null)
        jobInfo.setUsedResources(usedResources)

        ResourceSet askedResources = new ResourceSet(null, null, null, runLimit, null, queue, null)
        jobInfo.setAskedResources(askedResources)

        jobInfo.setUser(jobResult["USER"] as String ?: null)
        jobInfo.setDescription(jobResult["JOB_DESCRIPTION"] as String ?: null)
        jobInfo.setProjectName(jobResult["PROJ_NAME"] as String ?: null)
        jobInfo.setJobGroup(jobResult["JOB_GROUP"] as String ?: null)
        jobInfo.setPriority(jobResult["JOB_PRIORITY"] as String ?: null)
        jobInfo.setPidStr(jobResult["PIDS"] as String ?: null)
        jobInfo.setJobState(parseJobState(jobResult["STAT"] as String))
        jobInfo.setExitCode(jobInfo.jobState == JobState.COMPLETED_SUCCESSFUL ? 0 : (jobResult["EXIT_CODE"] ? Integer.valueOf(jobResult["EXIT_CODE"] as String) : null))
        jobInfo.setSubmissionHost(jobResult["FROM_HOST"] as String ?: null)
        jobInfo.setExecutionHosts(jobResult["EXEC_HOST"] as String ?: null)
        catchExceptionAndLog {
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
            catchExceptionAndLog { jobInfo.setSubmitTime(parseTime(jobResult["SUBMIT_TIME"] as String)) }
        if (jobResult["START_TIME"])
            catchExceptionAndLog { jobInfo.setStartTime(parseTime(jobResult["START_TIME"] as String)) }
        if (jobResult["FINISH_TIME"])
            catchExceptionAndLog {
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

    protected int getPositionOfJobID() {
        return 0
    }

    /**
     * Return the position of the status string within a stat result line. This changes if -u USERNAME is used!
     *
     * @return
     */
    protected int getPositionOfJobState() {
        if (isTrackingOfUserJobsEnabled)
            return 2
        return 2
    }

    @Override
    protected ExecutionResult executeKillJobs(List<BEJobID> jobIDs) {
        String command = "bkill ${jobIDs*.id.join(" ")}"
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
}

/*
//REST RESOURCES
static {

    URI_JOB_SUBMIT = "/jobs/submit"
    URI_JOB_KILL = "/jobs/kill"
    URI_JOB_SUSPEND = "/jobs/suspend"
    URI_JOB_RESUME = "/jobs/resume"
    URI_JOB_REQUEUE = "/jobs/requeue"
    URI_JOB_DETAILS = "/jobs/"
    URI_JOB_HISTORY = "/jobhistory"
    URI_USER_COMMAND = "/userCmd"
}

//PARAMETERS
enum Rest_Resources {

    URI_JOB_SUBMIT ("/jobs/submit"),
    URI_JOB_KILL ("/jobs/kill"),
    URI_JOB_SUSPEND ("/jobs/suspend"),
    URI_JOB_RESUME ("/jobs/resume"),
    URI_JOB_REQUEUE ("/jobs/requeue"),
    URI_JOB_DETAILS ("/jobs/"),
    URI_JOB_HISTORY ("/jobhistory"),
    URI_USER_COMMAND ("/userCmd"),

    final String value

    Rest_Resources(String value) {
        this.value = value
    }

    String getValue() {
        return this.value
    }

    String toString(){
        value
    }

    String getKey() {
        name()
    }
}

//PARAMETERS
enum Parameters {


    COMMANDTORUN("COMMANDTORUN"),
    EXTRA_PARAMS ("EXTRA_PARAMS"),
    MAX_MEMORY ("MAX_MEM"),
    MAX_NUMBER_CPU ("MAX_NUM_CPU"),
    MIN_NUMBER_CPU ("MIN_NUM_CPU"),
    PROC_PRE_HOST ("PROC_PRE_HOST"),
    EXTRA_RES ("EXTRA_RES"),
    RUN_LIMIT_MINUTE ("RUNLIMITMINUTE"),
    RERUNABLE ("RERUNABLE"),
    JOB_NAME ("JOB_NAME"),
    APP_PROFILE ("APP_PROFILE"),
    PROJECT_NAME ("PRJ_NAME"),
    RES_ID ("RES_ID"),
    LOGIN_SHELL ("LOGIN_SHELL"),
    QUEUE ("QUEUE"),
    INPUT_FILE ("INPUT_FILE"),
    OUTPUT_FILE ("OUTPUT_FILE"),
    ERROR_FILE ("ERROR_FILE"),

    final String value

    Parameters(String value) {

        this.value = value
    }


    String getValue() {

        return this.value
    }


    String toString(){

        value
    }


    String getKey() {

        name()
    }
}
 */
