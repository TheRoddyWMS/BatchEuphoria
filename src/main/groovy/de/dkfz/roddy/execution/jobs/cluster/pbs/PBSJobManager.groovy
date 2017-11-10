/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.JobLog
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
import java.util.Map.Entry
import java.util.concurrent.ExecutionException
import java.util.concurrent.locks.ReentrantLock
import java.util.regex.Matcher

/**
 * A job submission implementation for standard PBS systems.
 *
 * @author michael
 */
@groovy.transform.CompileStatic
class PBSJobManager extends ClusterJobManager<PBSCommand> {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(PBSJobManager.class.getSimpleName())

    public static final String PBS_JOBSTATE_RUNNING = "R"
    public static final String PBS_JOBSTATE_HOLD = "H"
    public static final String PBS_JOBSTATE_SUSPENDED = "S"
    public static final String PBS_JOBSTATE_QUEUED = "Q"
    public static final String PBS_JOBSTATE_TRANSFERED = "T"
    public static final String PBS_JOBSTATE_WAITING = "W"
    public static final String PBS_JOBSTATE_COMPLETED_UNKNOWN = "C"
    public static final String PBS_JOBSTATE_EXITING = "E"
    public static final String PBS_COMMAND_QUERY_STATES = "qstat -t"
    public static final String PBS_COMMAND_QUERY_STATES_FULL = "qstat -f"
    public static final String PBS_COMMAND_DELETE_JOBS = "qdel"
    public static final String PBS_LOGFILE_WILDCARD = "*.o"
    static public final String WITH_DELIMITER = '(?=(%1$s))'

    private Map<String, Boolean> mapOfInitialQueries = new LinkedHashMap<>()

    PBSJobManager(BEExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms)
        /**
         * General or specific todos for BatchEuphoriaJobManager and PBSJobManager
         */
        logger.rare("Need to find a way to properly get the job state for a completed job. Neither tracejob, nor qstat -f are a good way. qstat -f only works for 'active' jobs. Lists with long active lists are not default.")
        logger.rare("Set logfile location, parameter file and job state log file on job creation (or override a method).")
        logger.rare("Allow enabling and disabling of options for resource arbitration for defective job managers.")
        logger.rare("parseToJob() is not implemented and will return null.")
    }

    @Override
    PBSCommand createCommand(BEJob job, String jobName, List<ProcessingParameters> processingParameters, File tool, Map<String, String> parameters, List<String> parentJobs) {
//        PBSCommand pbsCommand = new PBSCommand(this, job, job.jobID, processingParameters, parameters, tags, arraySettings, dependencies, command, logDirectory)
//        return pbsCommand
        throw new NotImplementedException()
    }

    PBSCommand createCommand(BEJob job) {
        return new PBSCommand(this, job, job.jobName, [], job.parameters, [:], [], job.parentJobIDs*.id, job.tool?.getAbsolutePath() ?: job.getToolScript(), job.getLoggingDirectory())
    }

    @Override
    BEJobResult runJob(BEJob job) {
        def command = createCommand(job)
        def executionResult = executionService.execute(command)
        extractAndSetJobResultFromExecutionResult(command, executionResult)

        // job.runResult is set within executionService.execute
        // logger.severe("Set the job runResult in a better way from runJob itself or so.")
        cacheLock.lock()

        try {
            if (executionResult.successful && job.runResult.wasExecuted && job.jobManager.isHoldJobsEnabled()) {
                allStates[job.jobID] = JobState.HOLD
            } else if (executionResult.successful && job.runResult.wasExecuted) {
                allStates[job.jobID] = JobState.QUEUED
            } else {
                allStates[job.jobID] = JobState.FAILED
                logger.severe("PBS call failed with error code ${executionResult.exitCode} and error message:\n\t" + executionResult?.resultLines?.join("\n\t"))
            }
        } finally {
            cacheLock.unlock()
        }

        return job.runResult
    }

    /**
     * For BPS, we enable hold jobs by default.
     * If it is not enabled, we might run into the problem, that job dependencies cannot be
     * resolved early enough due to timing problems.
     * @return
     */
    @Override
    boolean getDefaultForHoldJobsEnabled() { return true }

    List<String> collectJobIDsFromJobs(List<BEJob> jobs) {
        BEJob.jobsWithUniqueValidJobId(jobs).collect { it.runResult.getJobID().shortID }
    }

    @Override
    void startHeldJobs(List<BEJob> heldJobs) {
        if (!isHoldJobsEnabled()) return
        if (!heldJobs) return
        String qrls = "qrls ${collectJobIDsFromJobs(heldJobs).join(" ")}"

        ExecutionResult er = executionService.execute(qrls)
        if(!er.successful){
            logger.warning("Hold jobs couldn't be started. \n status code: ${er.exitCode} \n result: ${er.resultLines}")
            throw new Exception("Hold jobs couldn't be started. \n status code: ${er.exitCode} \n result: ${er.resultLines}")
        }
    }

//
//    @Override
//    public ProcessingCommands getProcessingCommandsFromConfiguration(Configuration configuration, String toolID) {
//        ToolEntry toolEntry = configuration.getTools().getValue(toolID);
//        if (toolEntry.hasResourceSets()) {
//            return convertResourceSet(configuration, toolEntry.getResourceSet(configuration));
//        }
//        String resourceOptions = configuration.getConfigurationValues().getString(getResourceOptionsPrefix() + toolID, "");
//        if (resourceOptions.trim().length() == 0)
//            return null;
//
//        return convertPBSResourceOptionsString(resourceOptions);
//    }

    ProcessingParameters convertResourceSet(BEJob job, ResourceSet resourceSet) {
        assert resourceSet

        LinkedHashMultimap<String, String> parameters = LinkedHashMultimap.create()

        if (resourceSet.isMemSet()) parameters.put('-l', 'mem=' + resourceSet.getMem().toString(BufferUnit.M))
        if (resourceSet.isWalltimeSet()) parameters.put('-l', 'walltime=' + resourceSet.getWalltime())
        if (job?.customQueue) parameters.put('-q', job.customQueue)
        else if (resourceSet.isQueueSet()) parameters.put('-q', resourceSet.getQueue())

        if (resourceSet.isCoresSet() || resourceSet.isNodesSet()) {
            int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
            int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1
            // Currently not active
            String enforceSubmissionNodes = ''
            if (!enforceSubmissionNodes) {
                String pVal = 'nodes=' << nodes << ':ppn=' << cores
                if (resourceSet.isAdditionalNodeFlagSet()) {
                    pVal << ':' << resourceSet.getAdditionalNodeFlag()
                }
                parameters.put("-l", pVal)
            } else {
                String[] nodesArr = enforceSubmissionNodes.split(StringConstants.SPLIT_SEMICOLON)
                nodesArr.each {
                    String node ->
                        parameters.put('-l', 'nodes=' + node + ':ppn=' + resourceSet.getCores())
                }
            }
        }

        if (resourceSet.isStorageSet()) {
            parameters.put('-l', 'mem=' + resourceSet.getMem() + 'g')
        }

        return new ProcessingParameters(parameters)
    }

    String getResourceOptionsPrefix() {
        return "PBSResourceOptions_"
    }

    /**
     * #PBS -l walltime=8:00:00
     * #PBS -l nodes=1:ppn=12:lsdf
     * #PBS -S /bin/bash
     * #PBS -l mem=3600m
     * #PBS -m a
     *
     * #PBS -l walltime=50:00:00
     * #PBS -l nodes=1:ppn=6:lsdf
     * #PBS -l mem=52g
     * #PBS -m a
     * @param file
     * @return
     */
    @Override
    ProcessingParameters extractProcessingParametersFromToolScript(File file) {
        String[] text = RoddyIOHelperMethods.loadTextFile(file)

        List<String> lines = new LinkedList<String>()
        boolean preambel = true
        for (String line : text) {
            if (preambel && !line.startsWith("#PBS"))
                continue
            preambel = false
            if (!line.startsWith("#PBS"))
                break
            lines.add(line)
        }

        StringBuilder processingOptionsStr = new StringBuilder()
        for (String line : lines) {
            processingOptionsStr << " " << line.substring(5)
        }
        return ProcessingParameters.fromString(processingOptionsStr.toString())
    }

    /**
     * 120016, qsub -N r170402_171935425_A100_indelCalling -h
     *              -o /data/michael/temp/roddyLocalTest/testproject/rpp/A100/roddyExecutionStore/exec_170402_171935425_heinold_indelCalling
     *              -j oe  -l mem=16384M -l walltime=02:02:00:00 -l nodes=1:ppn=8
     *              -v PARAMETER_FILE=/data/michael/temp/roddyLocalTest/testproject/rpp/A100/roddyExecutionStore/exec_170402_171935425_heinold_indelCalling/r170402_171935425_A100_indelCalling_1.parameters
     *                 /data/michael/temp/roddyLocalTest/testproject/rpp/A100/roddyExecutionStore/exec_170402_171935425_heinold_indelCalling/analysisTools/indelCallingWorkflow/indelCalling.sh
     * @param commandString
     * @return
     */
    @Override
    BEJob parseToJob(String commandString) {
        GenericJobInfo jInfo = parseGenericJobInfo(commandString)
        BEJob job = new BEJob(
                new BEJobID(jInfo.id),
                jInfo.getJobName(),
                jInfo.getTool(),
                null,
                null,
                null,
                jInfo.getParentJobIDs().collect { new BEJob(new PBSJobID(it), this) },
                jInfo.getParameters(),
                this,
                JobLog.none(),
        )
        job.jobInfo = jInfo

        //Automatically get the status of the job and if it is planned or running add it as a job status listener.
//        String shortID = job.getJobID()
//        job.setJobState(queryJobStatus(Arrays.asList(shortID)).get(shortID))
//        if (job.getJobState().isPlannedOrRunning()) addJobStatusChangeListener(job)

        return job
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String commandString) {
        return new PBSCommandParser(commandString).toGenericJobInfo();
    }

    @Override
    BEJobResult convertToArrayResult(BEJob arrayChildJob, BEJobResult parentJobsResult, int arrayIndex) {
        return null
    }

    private static final ReentrantLock cacheLock = new ReentrantLock()

    private static ExecutionResult cachedExecutionResult

    protected Map<BEJobID, JobState> allStates = [:]

    protected Map<BEJobID, BEJob> jobStatusListeners = [:]

    /**
     * Queries the jobs states.
     *
     * @return
     */
    void updateJobStatus() {
        updateJobStatus(false)
    }

    protected String getQueryCommand() {
        return PBS_COMMAND_QUERY_STATES
    }

    protected void updateJobStatus(boolean forceUpdate) {

        if (!executionService.isAvailable())
            return

        String queryCommand = getQueryCommand()

        if (queryOnlyStartedJobs && listOfCreatedCommands.size() < 10) {
            for (Object _l : listOfCreatedCommands) {
                PBSCommand listOfCreatedCommand = (PBSCommand) _l
                queryCommand += " " + listOfCreatedCommand.getJob().getJobID()
            }
        }
        if (isTrackingOfUserJobsEnabled)
            queryCommand += " -u $userIDForQueries "


        Map<BEJobID, JobState> allStatesTemp = [:]
        ExecutionResult er
        List<String> resultLines = new LinkedList<String>()
        cacheLock.lock()
        try {
            if (forceUpdate || cachedExecutionResult == null || cachedExecutionResult.getAgeInSeconds() > 30) {
                cachedExecutionResult = executionService.execute(queryCommand.toString())
            }
        } finally {
            er = cachedExecutionResult
            resultLines.addAll(er.resultLines)

            cacheLock.unlock()
        }

        if (!er.successful) {
            if (strictMode) // Do not pull this into the outer if! The else branch needs to be executed if er.successful is true
                throw new BEException("The execution of ${queryCommand} failed.", null)
        } else {
            if (resultLines.size() > 2) {

                for (String line : resultLines) {
                    line = line.trim()
                    if (line.length() == 0) continue
                    if (!RoddyConversionHelperMethods.isInteger(line.substring(0, 1)))
                        continue //Filter out lines which have been missed which do not start with a number.

                    //TODO Put to a common class, is used multiple times.
                    line = line.replaceAll("\\s+", " ").trim()       //Replace multi white space with single whitespace
                    String[] split = line.split(" ")
                    final int ID = getPositionOfJobID()
                    final int JOBSTATE = getPositionOfJobState()
                    logger.info(["QStat BEJob line: " + line,
                                 "	Entry in arr[" + ID + "]: " + split[ID],
                                 "    Entry in arr[" + JOBSTATE + "]: " + split[JOBSTATE]].join("\n"))

                    String[] idSplit = split[ID].split("[.]")
                    BEJobID jobID  = new BEJobID(split[ID])
                    
                    //(idSplit.length <= 1) continue;
                    String id = idSplit[0]
                    JobState js = parseJobState(split[JOBSTATE])
                    allStatesTemp.put(jobID, js)
                    logger.info("   Extracted jobState: " + js.toString())
                }
            }

//            logger.severe("Reading out job states from job state logfiles is not possible yet!")

            // I don't currently know, if the jslisteners are used.
            //Create a local cache of jobstate logfile entries.
            Map<BEJobID, JobState> map = [:]
            List<BEJob> removejobs = new LinkedList<>()
            synchronized (jobStatusListeners) {
                for (BEJobID id : jobStatusListeners.keySet()) {
                    JobState js = allStatesTemp.get(id)
                    BEJob job = jobStatusListeners.get(id)

                    /*
                    if (js == JobState.UNKNOWN_SUBMITTED) {
                        // If the jobState is unknown and the job is not running anymore it is counted as failed.
                        job.setJobState(JobState.FAILED)
                        removejobs.add(job)
                        continue
                    }*/

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
                                map.each { BEJobID s, JobState v ->
                                    if (s.compareTo(id) == 0) {
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
        cacheLock.lock()
        allStates.clear()
//        allStatesTemp.each { String id, JobState status ->
        // The list is empty. We need to store the states for all found jobs by id again.
        // Queries will then use the id.
//            allStates[allStates.find { BEJob job, JobState state -> job.jobID == id }?.key] = status
//        }
        allStates.putAll(allStatesTemp)
        cacheLock.unlock()
    }

    @Override
    String getStringForQueuedJob() {
        return PBS_JOBSTATE_QUEUED
    }

    @Override
    String getStringForJobOnHold() {
        return PBS_JOBSTATE_HOLD
    }

    String getStringForJobSuspended() {
        return PBS_JOBSTATE_SUSPENDED
    }

    @Override
    String getStringForRunningJob() {
        return PBS_JOBSTATE_RUNNING
    }

    String getStringForCompletedJob() {
        return PBS_JOBSTATE_COMPLETED_UNKNOWN
    }

    @Override
    String getJobIdVariable() {
        return "PBS_JOBID"
    }

    @Override
    String getJobNameVariable() {
        return "PBS_JOBNAME"
    }

    @Override
    String getQueueVariable() {
        return 'PBS_QUEUE'
    }

    @Override
    String getNodeFileVariable() {
        return "PBS_NODEFILE"
    }

    @Override
    String getSubmitHostVariable() {
        return "PBS_O_HOST"
    }

    @Override
    String getSubmitDirectoryVariable() {
        return "PBS_O_WORKDIR"
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
            return 9
        return 4
    }

    @Override
    Map<BEJob, JobState> queryJobStatus(List<BEJob> jobs, boolean forceUpdate = false) {

        if (allStates == null || forceUpdate)
            updateJobStatus(forceUpdate)

        Map<BEJob, JobState> queriedStates = jobs.collectEntries { BEJob job -> [job, JobState.UNKNOWN] }

        for (BEJob job in jobs) {
            // Aborted somewhat supercedes everything.
            JobState state
            if (job.jobState == JobState.ABORTED)
                state = JobState.ABORTED
            else {
                cacheLock.lock()
                state = allStates[job.jobID]
                cacheLock.unlock()
            }
            if (state) queriedStates[job] = state
        }

        return queriedStates
    }

    @Override
    Map<BEJobID, JobState> queryJobStatusById(List<BEJobID> jobIds, boolean forceUpdate = false) {

        if (allStates == null || forceUpdate)
            updateJobStatus(forceUpdate)

        Map<BEJobID, JobState> queriedStates = jobIds.collectEntries { BEJobID jobId -> [jobId, JobState.UNKNOWN] }

        for (BEJobID jobId in jobIds) {
            JobState state
            cacheLock.lock()
            state = allStates[jobId]
            cacheLock.unlock()
            if (state) queriedStates[jobId] = state
        }

        return queriedStates
    }

    @Override
    Map<BEJobID, JobState> queryJobStatusAll(boolean forceUpdate = false) {

        if (allStates == null || forceUpdate)
            updateJobStatus(forceUpdate)

        Map<BEJobID, JobState> queriedStates = [:]
        cacheLock.lock()
        queriedStates.putAll(allStates)
        cacheLock.unlock()

        return queriedStates
    }

    @Override
    Map<BEJob, GenericJobInfo> queryExtendedJobState(List<BEJob> jobs, boolean forceUpdate) {

        Map<String, GenericJobInfo> queriedExtendedStates = queryExtendedJobStateById(jobs.collect {
            it.getJobID()
        }, false)
        return (Map<BEJob, GenericJobInfo>) queriedExtendedStates.collectEntries { Entry<String, GenericJobInfo> it -> [jobs.find { BEJob temp -> temp.getJobID() == it.key }, (GenericJobInfo) it.value] }
    }

    @Override
    Map<String, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds, boolean forceUpdate) {
        Map<String, GenericJobInfo> queriedExtendedStates = [:]
        String qStatCommand = PBS_COMMAND_QUERY_STATES_FULL
        if (isTrackingOfUserJobsEnabled)
            qStatCommand += " -u $userIDForQueries "

        qStatCommand += jobIds.collect { it }.join(" ")

        ExecutionResult er
        try {
            er = executionService.execute(qStatCommand.toString())
        } catch (Exception exp) {
            logger.severe("Could not execute qStat command", exp)
        }

        if (er != null && er.successful) {
            queriedExtendedStates = this.processQstatOutput(er.resultLines)
        }else{
            logger.postAlwaysInfo("Extended job states couldn't be retrieved. \n Returned status code:${er.exitCode} \n result:${er.resultLines}")
            throw new BEException("Extended job states couldn't be retrieved. \n Returned status code:${er.exitCode} \n result:${er.resultLines}")
        }
        return queriedExtendedStates
    }


    @Override
    void queryJobAbortion(List<BEJob> executedJobs) {
        def executionResult = executionService.execute("${PBS_COMMAND_DELETE_JOBS} ${collectJobIDsFromJobs(executedJobs).join(" ")}", false)
        if (executionResult.successful) {
            executedJobs.each { BEJob job -> job.jobState = JobState.ABORTED }
        } else {
            logger.always("Need to create a proper fail message for abortion.")
            throw new ExecutionException("Abortion of job states failed.", null)
        }
    }

    @Override
    void addJobStatusChangeListener(BEJob job) {
        synchronized (jobStatusListeners) {
            jobStatusListeners.put(job.getJobID(), job)
        }
    }

    @Override
    String getLogFileWildcard(BEJob job) {
        String id = job.getJobID()
        String searchID = id
        if (id == null) return null
        if (id.contains("[]"))
            return ""
        if (id.contains("[")) {
            String[] split = id.split("\\]")[0].split("\\[")
            searchID = split[0] + "-" + split[1]
        }
        return PBS_LOGFILE_WILDCARD + searchID
    }

    @Override
    String parseJobID(String commandOutput) {
        return commandOutput
    }

    @Override
    String getSubmissionCommand() {
        return PBSCommand.QSUB
    }

    /**
     * Reads qstat output
     * @param qstatOutput
     * @return output of qstat in a map with jobid as key
     */
    private Map<String, Map<String, String>> readQstatOutput(String qstatOutput) {
        return qstatOutput.split(String.format(WITH_DELIMITER, "\n\nJob Id: ")).collectEntries {
            Matcher matcher = it =~ /^\s*Job Id: (?<jobId>\d+)\..*\n/
            def result = new HashMap()
            if (matcher) {
                result[matcher.group("jobId")] = it
            }
            result
        }.collectEntries { jobId, value ->
            // join multi-line values
            value = ((String) value).replaceAll("\n\t", "")
            [(jobId): value]
        }.collectEntries { jobId, value ->
            Map<String, String> p = ((String) value).readLines().
                    findAll { it.startsWith("    ") && it.contains(" = ") }.
                    collectEntries {
                        String[] parts = it.split(" = ")
                        new MapEntry(parts.head().replaceAll(/^ {4}/, ""), parts.tail().join(' '))
                    }
            [(jobId): p]
        } as Map<String, Map<String, String>>
    }

    static LocalDateTime parseTime(String str) {
        def pbsDatePattern = DateTimeFormatter.ofPattern("EEE MMM ppd HH:mm:ss yyyy").withLocale(Locale.ENGLISH)
        return LocalDateTime.parse(str, pbsDatePattern)
    }

    /**
     * Reads the qstat output and creates GenericJobInfo objects
     * @param resultLines - Input of ExecutionResult object
     * @return map with jobid as key
     */
    private Map<String, GenericJobInfo> processQstatOutput(List<String> resultLines) {
        Map<String, GenericJobInfo> queriedExtendedStates = [:]
        Map<String, Map<String, String>> qstatReaderResult = this.readQstatOutput(resultLines.join("\n"))

        qstatReaderResult.each { it ->
                Map<String, String> jobResult = it.getValue()
                GenericJobInfo gj = new GenericJobInfo(jobResult.get("Job_Name"), null, it.getKey(), null, jobResult.get("depend") ? jobResult.get("depend").find("afterok.*")?.findAll(/(\d+).(\w+)/) { fullMatch, beforeDot, afterDot -> return beforeDot } : null)

                BufferValue mem = null
                Integer cores
                Integer nodes
                TimeUnit walltime = null
                String additionalNodeFlag

                if (jobResult.get("Resource_List.mem"))
                    mem = catchExceptionAndLog { new BufferValue(Integer.valueOf(jobResult.get("Resource_List.mem").find(/(\d+)/)), BufferUnit.valueOf(jobResult.get("Resource_List.mem")[-2])) }
                if (jobResult.get("Resource_List.nodect"))
                    nodes = catchExceptionAndLog { Integer.valueOf(jobResult.get("Resource_List.nodect")) }
                if (jobResult.get("Resource_List.nodes"))
                    cores = catchExceptionAndLog { Integer.valueOf(jobResult.get("Resource_List.nodes").find("ppn=.*").find(/(\d+)/)) }
                if (jobResult.get("Resource_List.nodes"))
                    additionalNodeFlag = catchExceptionAndLog { jobResult.get("Resource_List.nodes").find(/(\d+):(\.*)/) { fullMatch, nCores, feature -> return feature } }
                if (jobResult.get("Resource_List.walltime"))
                    walltime = catchExceptionAndLog { new TimeUnit(jobResult.get("Resource_List.walltime")) }

                BufferValue usedMem = null
                TimeUnit usedWalltime = null
                if (jobResult.get("resources_used.mem"))
                    catchExceptionAndLog { usedMem = new BufferValue(Integer.valueOf(jobResult.get("resources_used.mem").find(/(\d+)/)), BufferUnit.valueOf(jobResult.get("resources_used.mem")[-2]))  }
                if (jobResult.get("resources_used.walltime"))
                    catchExceptionAndLog { usedWalltime = new TimeUnit(jobResult.get("resources_used.walltime")) }

                gj.setAskedResources(new ResourceSet(null, mem, cores, nodes, walltime, null, jobResult.get("queue"), additionalNodeFlag))
                gj.setUsedResources(new ResourceSet(null, usedMem, null, null, usedWalltime, null, jobResult.get("queue"), null))

                gj.setOutFile(jobResult.get("Output_Path"))
                gj.setErrorFile(jobResult.get("Error_Path"))
                gj.setUser(jobResult.get("euser"))
                gj.setExecutionHosts(jobResult.get("exec_host"))
                gj.setSubmissionHost(jobResult.get("submit_host"))
                gj.setPriority(jobResult.get("Priority"))
                gj.setUserGroup(jobResult.get("egroup"))
                gj.setResourceReq(jobResult.get("submit_args"))
                gj.setRunTime(jobResult.get("total_runtime") ? catchExceptionAndLog { Duration.ofSeconds(Math.round(Double.parseDouble(jobResult.get("total_runtime"))), 0) } : null)
                gj.setCpuTime(jobResult.get("resources_used.cput") ? catchExceptionAndLog { parseColonSeparatedHHMMSSDuration(jobResult.get("resources_used.cput")) } : null)
                gj.setServer(jobResult.get("server"))
                gj.setUmask(jobResult.get("umask"))
                gj.setJobState(parseJobState(jobResult.get("job_state")))
                gj.setExitCode(jobResult.get("exit_status") ? catchExceptionAndLog { Integer.valueOf(jobResult.get("exit_status")) } : null)
                gj.setAccount(jobResult.get("Account_Name"))
                gj.setStartCount(jobResult.get("start_count") ? catchExceptionAndLog { Integer.valueOf(jobResult.get("start_count")) } : null)

                if (jobResult.get("qtime")) // The time that the job entered the current queue.
                    catchExceptionAndLog { gj.setSubmitTime(parseTime(jobResult.get("qtime"))) }
                if (jobResult.get("start_time")) // The timepoint the job was started.
                    catchExceptionAndLog { gj.setStartTime(parseTime(jobResult.get("start_time"))) }
                if (jobResult.get("comp_time"))  // The timepoint the job was completed.
                    catchExceptionAndLog { gj.setEndTime(parseTime(jobResult.get("comp_time"))) }
                if (jobResult.get("etime"))  // The time that the job became eligible to run, i.e. in a queued state while residing in an execution queue.
                    catchExceptionAndLog { gj.setEligibleTime(parseTime(jobResult.get("etime"))) }

                return queriedExtendedStates.put(it.getKey(), gj)

        }

        return queriedExtendedStates
    }

    @Override
    JobState parseJobState(String stateString) {
        JobState js = JobState.UNKNOWN
        if (stateString == PBS_JOBSTATE_RUNNING)
            js = JobState.RUNNING
        if (stateString == PBS_JOBSTATE_HOLD)
            js = JobState.HOLD
        if (stateString == PBS_JOBSTATE_SUSPENDED)
            js = JobState.SUSPENDED
        if (stateString == PBS_JOBSTATE_QUEUED || stateString == PBS_JOBSTATE_TRANSFERED || stateString == PBS_JOBSTATE_WAITING)
            js = JobState.QUEUED
        if (stateString == PBS_JOBSTATE_COMPLETED_UNKNOWN || stateString == PBS_JOBSTATE_EXITING) {
            js = JobState.COMPLETED_UNKNOWN
        }

        return js;
    }

    @Override
    List<String> getEnvironmentVariableGlobs() {
        return Collections.unmodifiableList(["PBS_*"])
    }

}
