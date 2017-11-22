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

import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.locks.ReentrantLock

/**
 * Factory for the management of LSF cluster systems.
 *
 *
 */
@groovy.transform.CompileStatic
class LSFJobManager extends AbstractLSFJobManager {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(LSFJobManager.class.getSimpleName())


    public static final String LSF_JOBSTATE_RUNNING = "RUN"
    public static final String LSF_JOBSTATE_HOLD = "PSUSP"
    public static final String LSF_JOBSTATE_QUEUED = "PEND"
    public static final String LSF_JOBSTATE_COMPLETED_SUCCESSFUL = "DONE"
    public static final String LSF_JOBSTATE_EXITING = "EXIT"
    public static final String LSF_COMMAND_QUERY_STATES = "bjobs -noheader -a -o \"jobid job_name stat user queue " +
            "job_description proj_name job_group job_priority pids exit_code from_host exec_host submit_time start_time " +
            "finish_time cpu_used run_time user_group swap max_mem runtimelimit sub_cwd " +
            "pend_reason exec_cwd output_file input_file effective_resreq exec_home slots error_file command dependency delimiter='<'\""
    public static final String LSF_COMMAND_DELETE_JOBS = "bkill"
    public static final String  LSF_LOGFILE_WILDCARD = "*.o"

    protected Map<BEJobID, JobState> allStates = [:]
    private static final ReentrantLock cacheLock = new ReentrantLock()

    protected String getQueryCommand() {
        return LSF_COMMAND_QUERY_STATES
    }

    @Override
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates = [:]
        for (BEJobID id : jobIds) {
            queriedExtendedStates.put(id,getJobInfo(id))
        }
        return queriedExtendedStates
    }

    @Override
    JobState parseJobState(String stateString) {
        JobState js = JobState.UNKNOWN

        if (stateString == getStringForRunningJob())
            js = JobState.RUNNING
        if (stateString == getStringForJobOnHold())
            js = JobState.SUSPENDED
        if (stateString == getStringForQueuedJob())
            js = JobState.QUEUED
        if (stateString == getStringForCompletedJob())
            js = JobState.COMPLETED_SUCCESSFUL
        if (stateString == getStringForFailedJob())
            js = JobState.FAILED

        return js
    }

    LSFJobManager(BEExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms)
    }

    LSFCommand createCommand(BEJob job) {
        return new LSFCommand(this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id, job.tool?.getAbsolutePath() ?: job.getToolScript())
    }

    @Override
    BEJobResult runJob(BEJob job) {
        def command = createCommand(job)
        ExecutionResult executionResult = executionService.execute(command)
        extractAndSetJobResultFromExecutionResult(command, executionResult)
        // job.runResult is set within executionService.execute
//        logger.severe("Set the job runResult in a better way from runJob itself or so.")
        try {
            cacheLock.lock()
            if (job.wasExecuted() && job.jobManager.isHoldJobsEnabled()) {
                allStates[job.jobID] = JobState.HOLD
            } else if (job.wasExecuted()) {
                allStates[job.jobID] = JobState.QUEUED
            } else {
                allStates[job.jobID] = JobState.FAILED
            }
            addJobStatusChangeListener(job)
        } finally {
            cacheLock.unlock()
        }
        return job.runResult
    }

    @Override
    boolean getDefaultForHoldJobsEnabled() { return false }

    List<String> collectJobIDsFromJobs(List<BEJob> jobs) {
        jobs.collect { it.runResult?.jobID?.shortID }.findAll { it }
    }

    @Override
    void startHeldJobs(List<BEJob> heldJobs) {
        if (!isHoldJobsEnabled()) return
        if (!heldJobs) return
        String qrls = "bresume ${collectJobIDsFromJobs(heldJobs).join(" ")}"

        ExecutionResult er = executionService.execute(qrls)
        if(!er.successful){
            logger.warning("Hold jobs couldn't be started. \n status code: ${er.exitCode} \n result: ${er.resultLines}")
            throw new Exception("Hold jobs couldn't be started. \n status code: ${er.exitCode} \n result: ${er.resultLines}")
        }
    }



    @Override
    ProcessingParameters extractProcessingParametersFromToolScript(File file) {
        String[] text = RoddyIOHelperMethods.loadTextFile(file)

        List<String> lines = new LinkedList<String>()
        boolean preambel = true
        for (String line : text) {
            if (preambel && !line.startsWith("#LSF"))
                continue
            preambel = false
            if (!line.startsWith("#LSF"))
                break
            lines.add(line)
        }

        StringBuilder processingOptionsStr = new StringBuilder()
        for (String line : lines) {
            processingOptionsStr << " " << line.substring(5)
        }
        return ProcessingParameters.fromString(processingOptionsStr.toString())
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String commandString) {
        return new LSFCommandParser(commandString).toGenericJobInfo();
    }

    protected Map<BEJobID, JobState> getJobStates(List<BEJobID> jobIDs) {
        runBjobs(jobIDs).collectEntries { BEJobID jobID, String[] value->
            JobState js = parseJobState(value[getPositionOfJobState()])
            if (logger.isVerbosityHigh())
                logger.postAlwaysInfo("   Extracted jobState: " + js.toString())
            [(jobID): js]
        } as Map<BEJobID, JobState>

    }

    private Map<BEJobID, String[]> runBjobs(List<BEJobID> jobIDs) {
        if (!executionService.isAvailable())
            return

        String queryCommand = getQueryCommand()

        if (jobIDs && listOfCreatedCommands.size() < 10) {
            queryCommand += " " + jobIDs*.id.join(" ")
        }

        if (isTrackingOfUserJobsEnabled)
            queryCommand += " -u $userIDForQueries "


        ExecutionResult er = executionService.execute(queryCommand.toString())
        List<String> resultLines = er.resultLines

        Map<BEJobID, String[]> result = [:]

        if (er.successful) {
            if (resultLines.size() >= 1) {

                for (String line : resultLines) {
                    line = line.trim()
                    if (line.length() == 0) continue
                    if (!RoddyConversionHelperMethods.isInteger(line.substring(0, 1)))
                        continue //Filter out lines which have been missed which do not start with a number.

                    //TODO Put to a common class, is used multiple times.
                    line = line.replaceAll("\\s+", " ").trim()       //Replace multi white space with single whitespace
                    String[] split = line.split("<")
                    final int ID = getPositionOfJobID()
                    final int JOBSTATE = getPositionOfJobState()
                    logger.info(["QStat BEJob line: " + line,
                                 "	Entry in arr[" + ID + "]: " + split[ID],
                                 "    Entry in arr[" + JOBSTATE + "]: " + split[JOBSTATE]].join("\n"))

                    result.put(new BEJobID(split[ID]), split)
                }
            }

        } else {
            logger.warning("Job status couldn't be updated. \n status code: ${er.exitCode} \n result: ${er.resultLines}")
            throw new BEException("Job status couldn't be updated. \n status code: ${er.exitCode} \n result: ${er.resultLines}")
        }
        return result
    }

    @Override
    String getJobIdVariable() {
        return "LSB_JOBID"
    }

    @Override
    String getJobNameVariable() {
        return "LSB_JOBNAME"
    }

    @Override
    String getQueueVariable() {
        return 'LSB_QUEUE'
    }

    @Override
    String getNodeFileVariable() {
        return "LSB_HOSTS"
    }

    @Override
    String getSubmitHostVariable() {
        return "LSB_SUB_HOST"
    }

    @Override
    String getSubmitDirectoryVariable() {
        return "LSB_SUBCWD"
    }


    static LocalDateTime parseTime(String str) {
        def datePattern = DateTimeFormatter.ofPattern("MMM ppd HH:mm yyyy").withLocale(Locale.ENGLISH)
        return LocalDateTime.parse(str, datePattern)
    }

    /**
     * Used by @getJobDetails to set JobInfo
     * @param job
     * @param jobDetails -
     */
    private GenericJobInfo getJobInfo(BEJobID jobID) {
        String[] jobDetails = runBjobs([jobID]).get(jobID)
        GenericJobInfo jobInfo

        List<String> dependIDs = jobDetails[32].tokenize(/&/).collect { it.find(/\d+/) }
        jobInfo = new GenericJobInfo(jobDetails[1], new File(jobDetails[31]), jobDetails[0], null, dependIDs)

        String[] jobResult = jobDetails.each { String property -> if (property.trim() == "-") return "" else property }

        String queue = !jobResult[16].toString().equals("-") ? jobResult[16] : null
        Duration runTime = catchExceptionAndLog { !jobResult[17].toString().equals("-") ? Duration.ofSeconds(Math.round(Double.parseDouble(jobResult[17].find("\\d+")))) : null }
        BufferValue swap = catchExceptionAndLog { !jobResult[19].toString().equals("-") ? new BufferValue(jobResult[19].find("\\d+"), BufferUnit.m) : null }
        BufferValue memory = catchExceptionAndLog { !jobResult[20].toString().equals("-") ? new BufferValue(jobResult[20].find("\\d+"), BufferUnit.m) : null }
        Duration runLimit = catchExceptionAndLog { !jobResult[21].toString().equals("-") ? Duration.ofSeconds(Math.round(Double.parseDouble(jobResult[21].find("\\d+")))) : null }
        Integer nodes = catchExceptionAndLog { !jobResult[29].toString().equals("-") ? jobResult[29].toString() as Integer : null }

        ResourceSet usedResources = new ResourceSet(memory, null, nodes, runTime, null, queue, null)
        jobInfo.setUsedResources(usedResources)

        ResourceSet askedResources = new ResourceSet(null, null, null, runLimit, null, queue, null)
        jobInfo.setAskedResources(askedResources)

        jobInfo.setUser(!jobResult[3].toString().equals("-") ? jobResult[3] : null)
        jobInfo.setDescription(!jobResult[5].toString().equals("-") ? jobResult[5] : null)
        jobInfo.setProjectName(!jobResult[6].toString().equals("-") ? jobResult[6] : null)
        jobInfo.setJobGroup(!jobResult[7].toString().equals("-") ? jobResult[7] : null)
        jobInfo.setPriority(!jobResult[8].toString().equals("-") ? jobResult[8] : null)
        jobInfo.setPidStr(!jobResult[9].toString().equals("-") ? jobResult[9] : null)
        jobInfo.setExitCode(!jobResult[10].toString().equals("-") ? Integer.valueOf(jobResult[10]) : null)
        jobInfo.setSubmissionHost(!jobResult[11].toString().equals("-") ? jobResult[11] : null)
        jobInfo.setExecutionHosts(!jobResult[12].toString().equals("-") ? jobResult[12] : null)
        catchExceptionAndLog { jobInfo.setCpuTime(!jobResult[16].toString().equals("-") ? parseColonSeparatedHHMMSSDuration(jobResult[16].toString()) : null) }
        jobInfo.setRunTime(runTime)
        jobInfo.setUserGroup(!jobResult[18].toString().equals("-") ? jobResult[18] : null)
        jobInfo.setCwd(!jobResult[22].toString().equals("-") ? jobResult[22] : null)
        jobInfo.setPendReason(!jobResult[23].toString().equals("-") ? jobResult[23] : null)
        jobInfo.setExecCwd(!jobResult[24].toString().equals("-") ? jobResult[24] : null)
        jobInfo.setOutFile(getBjobsFile(jobResult[25],jobID , "out"))
        jobInfo.setErrorFile(getBjobsFile(jobResult[30], jobID, "err"))
        jobInfo.setInFile(!jobResult[26].toString().equals("-") ? new File(jobResult[26]) : null)
        jobInfo.setResourceReq(!jobResult[27].toString().equals("-") ? jobResult[27] : null)
        jobInfo.setExecHome(!jobResult[28].toString().equals("-") ? jobResult[28] : null)

        if (!jobResult[13].toString().equals("-"))
            catchExceptionAndLog { jobInfo.setSubmitTime(parseTime(jobResult[13] + " " + LocalDateTime.now().getYear())) }
        if (!jobResult[14].toString().equals("-"))
            catchExceptionAndLog { jobInfo.setStartTime(parseTime(jobResult[14] + " " + LocalDateTime.now().getYear())) }
        if (!jobResult[15].toString().equals("-"))
            catchExceptionAndLog { jobInfo.setEndTime(parseTime(jobResult[15] + " " + LocalDateTime.now().getYear())) }

        return jobInfo
    }

    private File getBjobsFile(String s, BEJobID jobID, String type) {
        if (!s || s == "-") {
            return null
        } else if (executionService.execute("stat -c %F ${Command.escapeBash(s)}").firstLine == "directory") {
            return new File(s, "${jobID.getId()}.${type}")
        } else {
            return new File(s)
        }
    }


    @Override
    String getStringForQueuedJob() {
        return LSF_JOBSTATE_QUEUED
    }

    @Override
    String getStringForJobOnHold() {
        return LSF_JOBSTATE_HOLD
    }

    @Override
    String getStringForRunningJob() {
        return LSF_JOBSTATE_RUNNING
    }

    String getStringForFailedJob() {
        return LSF_JOBSTATE_EXITING
    }

    String getStringForCompletedJob() {
        return LSF_JOBSTATE_COMPLETED_SUCCESSFUL
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
    void queryJobAbortion(List<BEJob> executedJobs) {
        logger.always("${LSF_COMMAND_DELETE_JOBS} ${collectJobIDsFromJobs(executedJobs).join(" ")}")
        def executionResult = executionService.execute("${LSF_COMMAND_DELETE_JOBS} ${collectJobIDsFromJobs(executedJobs).join(" ")}", false)

        if (executionResult.successful) {
            executedJobs.each { BEJob job -> job.jobState = JobState.ABORTED }
        } else {
            logger.warning("Job couldn't be aborted. \n status code: ${executionResult.exitCode} \n result: ${executionResult.resultLines}")
            throw new BEException("Job couldn't be aborted. \n status code: ${executionResult.exitCode} \n result: ${executionResult.resultLines}")
        }
    }

    @Override
    String getLogFileWildcard(BEJob job) {
        String id = job.getJobID()
        return LSF_LOGFILE_WILDCARD + id
    }


    @Override
    String parseJobID(String commandOutput) {
        String result = commandOutput.find("<[0-9]+>")
        if (result == null)
            throw new BEException("Could not parse raw ID from: '${commandOutput}'")
        String exID = result.substring(1, result.length() - 1)
        return exID
    }

    @Override
    String getSubmissionCommand() {
        return LSFCommand.BSUB
    }

    @Override
    List<String> getEnvironmentVariableGlobs() {
        return Collections.unmodifiableList(["LSB_*", "LS_*"])
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
