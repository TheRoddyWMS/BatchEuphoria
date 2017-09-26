/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.ClusterJobManager
import de.dkfz.roddy.tools.*
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.locks.ReentrantLock

import static de.dkfz.roddy.StringConstants.*

/**
 * Factory for the management of LSF cluster systems.
 *
 *
 */
@groovy.transform.CompileStatic
class LSFJobManager extends ClusterJobManager<LSFCommand> {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(LSFJobManager.class.getSimpleName())


    public static final String LSF_JOBSTATE_RUNNING = "RUN"
    public static final String LSF_JOBSTATE_HOLD = "PSUSP"
    public static final String LSF_JOBSTATE_QUEUED = "PEND"
    public static final String LSF_JOBSTATE_COMPLETED_SUCCESSFUL = "DONE"
    public static final String LSF_JOBSTATE_EXITING = "EXIT"
    public static final String LSF_COMMAND_QUERY_STATES = "bjobs -noheader -o \"jobid job_name stat user queue " +
            "job_description proj_name job_group job_priority pids exit_code from_host exec_host submit_time start_time " +
            "finish_time cpu_used run_time user_group swap max_mem runtimelimit sub_cwd " +
            "pend_reason exec_cwd output_file input_file effective_resreq exec_home slots delimiter='<'\""
    public static final String LSF_COMMAND_DELETE_JOBS = "bkill"
    public static final String LSF_LOGFILE_WILDCARD = "*.o"

    protected Map<String, JobState> allStates = [:]
    private static final ReentrantLock cacheLock = new ReentrantLock()

    protected Map<String, BEJob> jobStatusListeners = [:]

    private static ExecutionResult cachedExecutionResult

    protected String getQueryCommand() {
        return LSF_COMMAND_QUERY_STATES
    }

    @Override
    Map<String, JobState> queryJobStatusById(List<String> jobIds, boolean forceUpdate = false) {

        if (allStates == null || forceUpdate)
            updateJobStatus(forceUpdate)

        Map<String, JobState> queriedStates = jobIds.collectEntries { String jobId -> [jobId, JobState.UNKNOWN] }

        for (String jobId in jobIds) {
            JobState state
            cacheLock.lock()
            state = allStates[jobId]
            cacheLock.unlock()
            if (state) queriedStates[jobId] = state
        }

        return queriedStates
    }

    @Override
    Map<String, JobState> queryJobStatusAll(boolean forceUpdate = false) {

        if (allStates == null || forceUpdate)
            updateJobStatus(forceUpdate)

        Map<String, JobState> queriedStates = [:]
        cacheLock.lock()
        queriedStates.putAll(allStates)
        cacheLock.unlock()

        return queriedStates
    }

    @Override
    Map<BEJob, GenericJobInfo> queryExtendedJobState(List<BEJob> jobs, boolean forceUpdate) {

        Map<String, GenericJobInfo> queriedExtendedStates = queryExtendedJobStateById(jobs.collect {
            it.getJobID().toString()
        }, false)
        return (Map<BEJob, GenericJobInfo>) queriedExtendedStates.collectEntries { Map.Entry<String, GenericJobInfo> it -> [jobs.find { BEJob temp -> temp.getJobID() == it.key }, (GenericJobInfo) it.value] }
    }

    @Override
    Map<String, GenericJobInfo> queryExtendedJobStateById(List<String> jobIds, boolean forceUpdate) {
        Map<String, GenericJobInfo> queriedExtendedStates = [:]
        updateJobStatus()
        for (String id : jobIds) {
            Map.Entry<String, BEJob> job = jobStatusListeners.find { it.key == id }
            if (job)
                queriedExtendedStates.put(job.getKey(), job.getValue().getJobInfo())
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
        return new LSFCommand(this, job, job.jobName, [], job.parameters, [:], [], job.parentJobIDsAsString, job.tool?.getAbsolutePath() ?: job.getToolScript(), job.loggingDirectory)
    }

    @Override
    LSFCommand createCommand(BEJob job, String jobName, List<ProcessingParameters> processingParameters, File tool, Map<String, String> parameters, List<String> parentJobs) {
        throw new NotImplementedException()
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
                allStates[job.jobID.getId()] = JobState.HOLD
            } else if (job.wasExecuted()) {
                allStates[job.jobID.getId()] = JobState.QUEUED
            } else {
                allStates[job.jobID.getId()] = JobState.FAILED
            }
            jobStatusListeners.put(job.jobID.getId(), job)
        } finally {
            cacheLock.unlock()
        }
        return job.runResult
    }

    /**
     * Called by the execution service after a command was executed.
     */
    @Override
    BEJobResult extractAndSetJobResultFromExecutionResult(Command command, ExecutionResult res) {
        BEJobResult jobResult
        if (res.successful) {
            String rawId = res.resultLines[0].find("<[0-9]*>")
            String exID = rawId.substring(1, rawId.length() - 1)
            def job = command.getJob()
            BEJobID jobID = new BEJobID(exID)
            job.resetJobID(jobID)
            command.setExecutionID(jobID)
            jobResult = new BEJobResult(command, job, res, false, job.tool, job.parameters, job.parentJobs as List<BEJob>)
            job.setRunResult(jobResult)
        } else {
            def job = command.getJob()
            jobResult = new BEJobResult(command, job, res, false, job.tool, job.parameters, job.parentJobs as List<BEJob>)
            job.setRunResult(jobResult)
        }
        return jobResult
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
        executionService.execute(qrls)
    }

    @Override
    ProcessingParameters convertResourceSet(BEJob job, ResourceSet resourceSet) {
        LinkedHashMultimap<String, String> resourceParameters = LinkedHashMultimap.create()
        if (resourceSet.isQueueSet()) {
            resourceParameters.put("-q", resourceSet.getQueue())
        }
        if (resourceSet.isMemSet()) {
            String memo = resourceSet.getMem().toString(BufferUnit.M)
            resourceParameters.put("-M", memo.substring(0, memo.toString().length() - 1))
        }
        if (resourceSet.isWalltimeSet()) {
            resourceParameters.put("-W", durationToLSFWallTime(resourceSet.getWalltimeAsDuration()))
        }
        if (resourceSet.isCoresSet() || resourceSet.isNodesSet()) {
            int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
            resourceParameters.put("-n", nodes.toString())
        }
        return new ProcessingParameters(resourceParameters)
    }

    private String durationToLSFWallTime(Duration wallTime) {
        if (wallTime) {
            return String.valueOf(wallTime.toMinutes())
        }
        return null
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
    BEJob parseToJob(String commandString) {
//        return null
        GenericJobInfo jInfo = parseGenericJobInfo(commandString)
        //BEJob job = new BEJob(jInfo.getJobName(), jInfo.getTool(), null, "", null, [], jInfo.getParameters(), null, jInfo.getParentJobIDs().collect {
        //      new BEJobID(null, job)
        //  } as List<de.dkfz.roddy.execution.jobs.BEJobDependencyID>, this);

        //Autmatically get the status of the job and if it is planned or running add it as a job status listener.
//        String shortID = job.getJobID()
//        job.setJobState(queryJobStatus(Arrays.asList(shortID)).get(shortID))
//        if (job.getJobState().isPlannedOrRunning()) addJobStatusChangeListener(job)

        return null
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String commandString) {
        return new LSFCommandParser(commandString).toGenericJobInfo();
    }

    @Override
    BEJobResult convertToArrayResult(BEJob arrayChildJob, BEJobResult parentJobsResult, int arrayIndex) {
        return null
    }

    @Override
    /**
     * Queries the jobs states.
     *
     * @return
     */
    void updateJobStatus() {
        updateJobStatus(false)
    }

    protected void updateJobStatus(boolean forceUpdate) {

        if (!executionService.isAvailable())
            return

        String queryCommand = getQueryCommand()

        if (queryOnlyStartedJobs && listOfCreatedCommands.size() < 10) {
            for (Object _l : listOfCreatedCommands) {
                LSFCommand listOfCreatedCommand = (LSFCommand) _l
                queryCommand += " " + listOfCreatedCommand.getJob().getJobID()
            }
        }
        if (isTrackingOfUserJobsEnabled)
            queryCommand += " -u $userIDForQueries "


        Map<String, List> allStatesTemp = [:]
        ExecutionResult er
        List<String> resultLines = new LinkedList<String>()
        cacheLock.lock()

        try {
            if (forceUpdate || cachedExecutionResult == null || cachedExecutionResult.getAgeInSeconds() > 30) {
                cachedExecutionResult = executionService.execute(queryCommand)
            }
        } catch (Exception ex) {
            ex.printStackTrace()

        }
        er = cachedExecutionResult
        resultLines.addAll(er.resultLines)

        cacheLock.unlock()

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
                    if (logger.isVerbosityHigh()) {
                        System.out.println("QStat Job line: " + line)
                        System.out.println("	Entry in arr[" + ID + "]: " + split[ID])
                        System.out.println("    Entry in arr[" + JOBSTATE + "]: " + split[JOBSTATE])
                    }

                    String[] idSplit = split[ID].split("[.]")
                    //(idSplit.length <= 1) continue;
                    String id = idSplit[0]
                    JobState js = JobState.UNKNOWN
                    logger.severe(line)
                    logger.severe(split.toString())
                    logger.severe(id + " " + split[JOBSTATE])
                    js = parseJobState(split[JOBSTATE])
                    logger.severe(js.toString())
                    allStatesTemp.put(id, [js, split])

                    if (logger.isVerbosityHigh())
                        System.out.println("   Extracted jobState: " + js.toString())
                }
            }

//            logger.severe("Reading out job states from job state logfiles is not possible yet!")

            // I don't currently know, if the jslisteners are used.
            //Create a local cache of jobstate logfile entries.
            Map<String, JobState> map = [:]
            List<BEJob> removejobs = new LinkedList<>()
            synchronized (jobStatusListeners) {
                for (String id : jobStatusListeners.keySet()) {
                    if (allStatesTemp.get(id)) {
                        JobState js = (JobState) allStatesTemp.get(id)[0]
                        BEJob job = jobStatusListeners.get(id)
                        setJobInfoForJobDetails(job, (String[]) allStatesTemp.get(id)[1])
                        logger.severe("id:" + id + " jobstate: " + js.toString() + " jobInfo: " + job.getJobInfo().toString())
                        if (js == JobState.UNKNOWN) {
                            // If the jobState is unknown and the job is not running anymore it is counted as failed.
                            job.setJobState(JobState.FAILED)
                            removejobs.add(job)
                            continue
                        }

                        if (JobState._isPlannedOrRunning(js)) {
                            job.setJobState(js)
                            continue
                        }

                        if (job.getJobState() == JobState.FAILED)
                            continue
                        //Do not query jobs again if their status is already final. TODO Remove final jobs from listener list?

                        if (js == null || js == JobState.UNKNOWN) {
                            //Read from jobstate logfile.
                            try {
//                            ExecutionContext executionContext = job.getExecutionContext()
//                            if (!map.containsKey(executionContext))
//                                map.put(executionContext, executionContext.readJobStateLogFile())

                                JobState jobsCurrentState = null
                                if (job.getRunResult() != null) {
                                    jobsCurrentState = map.get(job.getRunResult().getJobID().getId())
                                } else { //Search within entries.
                                    map.each { String s, JobState v ->
                                        if (s.startsWith(id)) {
                                            jobsCurrentState = v
                                            return
                                        }
                                    }
                                }
                                js = jobsCurrentState
                            } catch (Exception ex) {
                                //Could not read out job jobState from file
                            }
                        }
                        job.setJobState(js)
                    }
                }
            }
        }
        cacheLock.lock()
        allStates.clear()
//        allStatesTemp.each { String id, JobState status ->
        // The list is empty. We need to store the states for all found jobs by id again.
        // Queries will then use the id.
//            allStates[allStates.find { Job job, JobState state -> job.jobID == id }?.key] = status
//        }
        allStates.putAll((Map<String, JobState>) allStatesTemp.collectEntries {
            new MapEntry(it.key, (JobState) it.value[0])
        })
        cacheLock.unlock()
    }

    static LocalDateTime parseTime(String str) {
        def datePattern = DateTimeFormatter.ofPattern("MMM ppd HH:mm yyyy").withLocale(Locale.ENGLISH)
        return LocalDateTime.parse(str, datePattern)
    }

    /**
     * Used by @getJobDetails to set JobInfo
     * @param job
     * @param jobDetails - XML job details
     */
    void setJobInfoForJobDetails(BEJob job, String[] jobDetails) {
        GenericJobInfo jobInfo

        if (job.getJobInfo() != null) {
            jobInfo = job.getJobInfo()
        } else {
            jobInfo = new GenericJobInfo(jobDetails[1], job.getTool(), jobDetails[0], job.getParameters(), job.getParentJobIDsAsString())
        }
        String[] jobResult = jobDetails.each { String property -> if (property.trim() == "-") return "" else property }

        String queue = !jobResult[16].toString().equals("-") ? jobResult[16] : null
        Duration runTime = !jobResult[17].toString().equals("-") ? Duration.ofSeconds(Math.round(Double.parseDouble(jobResult[17].find("\\d+")))) : null
        BufferValue swap = !jobResult[19].toString().equals("-") ? new BufferValue(jobResult[19].find("\\d+"), BufferUnit.m) : null
        BufferValue memory = !jobResult[20].toString().equals("-") ? new BufferValue(jobResult[20].find("\\d+"), BufferUnit.m) : null
        Duration runLimit = !jobResult[21].toString().equals("-") ? Duration.ofSeconds(Math.round(Double.parseDouble(jobResult[21].find("\\d+")))) : null
        Integer nodes = !jobResult[29].toString().equals("-") ? jobResult[29].toString() as Integer : null

        ResourceSet usedResources = new ResourceSet(memory, null, nodes, runTime, null, queue, null)
        jobInfo.setUsedResources(usedResources)

        ResourceSet askedResources = new ResourceSet(null, null, null, runLimit, null, queue, null)

        jobInfo.setAskedResources(askedResources)
        jobInfo.setUsedResources(usedResources)
        jobInfo.setUser(!jobResult[3].toString().equals("-") ? jobResult[3] : null)
        jobInfo.setDescription(!jobResult[5].toString().equals("-") ? jobResult[5] : null)
        jobInfo.setProjectName(!jobResult[6].toString().equals("-") ? jobResult[6] : null)
        jobInfo.setJobGroup(!jobResult[7].toString().equals("-") ? jobResult[7] : null)
        jobInfo.setPriority(!jobResult[8].toString().equals("-") ? jobResult[8] : null)
        jobInfo.setPidStr(!jobResult[9].toString().equals("-") ? jobResult[9] : null)
        jobInfo.setExitCode(!jobResult[10].toString().equals("-") ? Integer.valueOf(jobResult[10]) : null)
        jobInfo.setSubmissionHost(!jobResult[11].toString().equals("-") ? jobResult[11] : null)
        jobInfo.setExecutionHosts(!jobResult[12].toString().equals("-") ? jobResult[12] : null)
        jobInfo.setCpuTime(!jobResult[16].toString().equals("-") ? parseColonSeparatedHHMMSSDuration(jobResult[16].toString()) : null)
        jobInfo.setRunTime(runTime)
        jobInfo.setUserGroup(!jobResult[18].toString().equals("-") ? jobResult[18] : null)
        jobInfo.setCwd(!jobResult[22].toString().equals("-") ? jobResult[22] : null)
        jobInfo.setPendReason(!jobResult[23].toString().equals("-") ? jobResult[23] : null)
        jobInfo.setExecCwd(!jobResult[24].toString().equals("-") ? jobResult[24] : null)
        jobInfo.setOutFile(!jobResult[25].toString().equals("-") ? jobResult[25] : null)
        jobInfo.setInFile(!jobResult[26].toString().equals("-") ? jobResult[26] : null)
        jobInfo.setResourceReq(!jobResult[27].toString().equals("-") ? jobResult[27] : null)
        jobInfo.setExecHome(!jobResult[28].toString().equals("-") ? jobResult[28] : null)
        job.setJobInfo(jobInfo)

        if (!jobResult[13].toString().equals("-"))
            jobInfo.setSubmitTime(parseTime(jobResult[13] + " " + LocalDateTime.now().getYear()))
        if (!jobResult[14].toString().equals("-"))
            jobInfo.setStartTime(parseTime(jobResult[14] + " " + LocalDateTime.now().getYear()))
        if (!jobResult[15].toString().equals("-"))
            jobInfo.setEndTime(parseTime(jobResult[15] + " " + LocalDateTime.now().getYear()))

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

    @Override
    String getJobIdVariable() {
        return "LSB_JOBID"
    }

    @Override
    String getJobArrayIndexVariable() {
        return "LSB_JOBINDEX"
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
    Map<BEJob, JobState> queryJobStatus(List<BEJob> jobs, boolean forceUpdate = false) {

        if (allStates == null || forceUpdate)
            updateJobStatus(forceUpdate)

        Map<BEJob, JobState> queriedStates = jobs.collectEntries { BEJob job -> [job, JobState.UNKNOWN] }

        for (BEJob job in jobs) {
            JobState state
            if (job.jobState == JobState.ABORTED)
                state = JobState.ABORTED
            else {
                cacheLock.lock()
                try {
                    state = allStates[job.getJobID().getId()]
                } finally {
                    cacheLock.unlock()
                }
            }
            if (state) queriedStates[job] = state
        }

        return queriedStates
    }


    @Override
    void queryJobAbortion(List<BEJob> executedJobs) {
        logger.always("${LSF_COMMAND_DELETE_JOBS} ${collectJobIDsFromJobs(executedJobs).join(" ")}")
        def executionResult = executionService.execute("${LSF_COMMAND_DELETE_JOBS} ${collectJobIDsFromJobs(executedJobs).join(" ")}", false)

        if (executionResult.successful) {
            executedJobs.each { BEJob job -> job.jobState = JobState.ABORTED }
        } else {
            logger.always("Need to create a proper fail message for abortion.")
            throw new RuntimeException("Abortion of job states failed.")
        }
    }

    @Override
    void addJobStatusChangeListener(BEJob job) {
        synchronized (jobStatusListeners) {
            jobStatusListeners.put(job.getJobID().getId(), job)
        }
    }

    @Override
    String getLogFileWildcard(BEJob job) {
        String id = job.getJobID()
        return LSF_LOGFILE_WILDCARD + id
    }

//    /**
//     * Returns the path to the jobs logfile (if existing). Otherwise null.
//     * Throws a runtime exception if more than one logfile exists.
//     *
//     * @param readOutJob
//     * @return
//     */
//    @Override
//    public File getLogFileForJob(ReadOutJob readOutJob) {
//        List<File> files = Roddy.getInstance().listFilesInDirectory(readOutJob.context.getExecutionDirectory(), Arrays.asList("*" + readOutJob.getJobID()));
//        if (files.size() > 1)
//            throw new RuntimeException("There should only be one logfile for this job: " + readOutJob.getJobID());
//        if (files.size() == 0)
//            return null;
//        return files.get(0);
//    }

    @Override
    boolean compareJobIDs(String jobID, String id) {
        if (jobID.length() == id.length()) {
            return jobID == id
        } else {
            String id0 = jobID.split("[.]")[0]
            String id1 = id.split("[.]")[0]
            return id0 == id1
        }
    }

    @Override
    String[] peekLogFile(BEJob job) {
        String user = userIDForQueries
        String id = job.getJobID()
        String searchID = id
        if (id.contains(SBRACKET_LEFT)) {
            String[] split = id.split(SPLIT_SBRACKET_RIGHT)[0].split(SPLIT_SBRACKET_LEFT)
            searchID = split[0] + MINUS + split[1]
        }
        /*
        String cmd = String.format("jobHost=`bjobs -f %s  | grep exec_host | cut -d \"/\" -f 1 | cut -d \"=\" -f 2`; ssh %s@${jobHost: 1} 'cat /opt/torque/spool/spool/*'%s'*'", id, user, searchID)
        ExecutionResult executionResult = executionService.execute(cmd)
        if (executionResult.successful)
            return executionResult.resultLines.toArray(new String[0])*/
        return new String[0]
    }

    @Override
    String parseJobID(String commandOutput) {
        return commandOutput
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
