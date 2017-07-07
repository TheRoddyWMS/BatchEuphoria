/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.cluster.pbs

import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.cluster.ClusterJobManager
import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.JobDependencyID
import de.dkfz.roddy.execution.jobs.JobManagerCreationParameters
import de.dkfz.roddy.execution.jobs.JobResult
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.execution.jobs.ProcessingCommands
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.LoggerWrapper
import de.dkfz.roddy.tools.RoddyConversionHelperMethods
import de.dkfz.roddy.tools.RoddyIOHelperMethods
import de.dkfz.roddy.tools.TimeUnit
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Map.Entry
import java.util.concurrent.locks.ReentrantLock
import java.util.regex.Pattern

import static de.dkfz.roddy.StringConstants.*

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
    public static final String PBS_JOBSTATE_QUEUED = "Q"
    public static final String PBS_JOBSTATE_COMPLETED_UNKNOWN = "C"
    public static final String PBS_JOBSTATE_EXITING = "E"
    public static final String PBS_COMMAND_QUERY_STATES = "qstat -t"
    public static final String PBS_COMMAND_QUERY_STATES_FULL = "qstat -f"
    public static final String PBS_COMMAND_DELETE_JOBS = "qdel"
    public static final String PBS_LOGFILE_WILDCARD = "*.o"
    public static final String PBS_JOBID = '${PBS_JOBID}'
    public static final String PBS_ARRAYID = '${PBS_ARRAYID}'
    public static final String PBS_SCRATCH = '${PBS_SCRATCH_DIR}/${PBS_JOBID}'

    private Map<String, Boolean> mapOfInitialQueries = new LinkedHashMap<>()

    PBSJobManager(BEExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms)
        /**
         * General or specific todos for BatchEuphoriaJobManager and PBSJobManager
         */
        logger.severe("Need to find a way to properly get the job state for a completed job. Neither tracejob, nor qstat -f are a good way. qstat -f only works for 'active' jobs. Lists with long active lists are not default.")
        logger.severe("Set logfile location, parameter file and job state log file on job creation (or override a method).")
        logger.severe("Allow enabling and disabling of options for resource arbitration for defective job managers.")
        logger.severe("parseToJob() is not implemented and will return null.")
    }
// Will not work in first implementation. This constructor was used in the transformation process from one Batch system to another one (e.g. PBS => SGE)
//    @Override
//    PBSCommand createCommand(GenericJobInfo jobInfo) {
//        BEJob job = new BEJob(jobInfo.getJobName(), jobInfo.getTool(), new LinkedHashMap<String, Object>(jobInfo.getParameters()));
//        PBSCommand pbsCommand = new PBSCommand(job, jobInfo.getExecutionContext(), BEExecutionService.getInstance(), job.getJobName(), null, job.getParameters(), null, jobInfo.getParentJobIDs(), jobInfo.getExecutionContext().getConfiguration().getProcessingToolPath(jobInfo.getExecutionContext(), jobInfo.getTool()).getAbsolutePath());
//        return pbsCommand;
//    }

    @Override
    PBSCommand createCommand(GenericJobInfo jobInfo) {
        throw new NotImplementedException()
    }

//    @Override
    PBSCommand createCommand(BEJob job, List<ProcessingCommands> processingCommands, String command, Map<String, String> parameters, Map<String, Object> tags, List<String> dependencies, List<String> arraySettings, File logDirectory) {
        PBSCommand pbsCommand = new PBSCommand(this, job, job.jobID, processingCommands, parameters, tags, arraySettings, dependencies, command, logDirectory)
        return pbsCommand
    }

    @Override
    PBSCommand createCommand(BEJob job, String jobName, List<ProcessingCommands> processingCommands, File tool, Map<String, String> parameters, List<String> dependencies, List<String> arraySettings) {
//        PBSCommand pbsCommand = new PBSCommand(this, job, job.jobID, processingCommands, parameters, tags, arraySettings, dependencies, command, logDirectory)
//        return pbsCommand
        throw new NotImplementedException()
    }

    PBSCommand createCommand(BEJob job) {
        return new PBSCommand(this, job, job.jobName, [], job.parameters, [:], [], job.dependencyIDsAsString, job.tool?.getAbsolutePath() ?: job.getToolScript(), job.getLoggingDirectory())
    }

    @Override
    JobResult runJob(BEJob job) {
        def command = createCommand(job)
        def executionResult = executionService.execute(command)
        extractAndSetJobResultFromExecutionResult(command, executionResult)
        executionService.handleServiceBasedJobExitStatus(command, executionResult, null)

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
     * ressolved early enough due to timing problems.
     * @return
     */
    @Override
    boolean getDefaultForHoldJobsEnabled() { return true }

    List<String> collectJobIDsFromJobs(List<BEJob> jobs) {
        jobs.collect { it.runResult?.jobID?.shortID }.findAll { it }
    }

    @Override
    void startHeldJobs(List<BEJob> heldJobs) {
        if (!isHoldJobsEnabled()) return
        if (!heldJobs) return
        String qrls = "qrls ${collectJobIDsFromJobs(heldJobs).join(" ")}"
        executionService.execute(qrls)
    }

    @Override
    JobDependencyID createJobDependencyID(BEJob job, String jobResult) {
        return new PBSJobDependencyID(job, jobResult)
    }

    @Override
    ProcessingCommands parseProcessingCommands(String processingString) {
        return convertPBSResourceOptionsString(processingString)
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

    @Override
    ProcessingCommands convertResourceSet(BEJob job) {
        return convertResourceSet(job, job.resourceSet)
    }

    @Override
    ProcessingCommands convertResourceSet(ResourceSet resourceSet) {
        return convertResourceSet(null, resourceSet)
    }

    ProcessingCommands convertResourceSet(BEJob job, ResourceSet resourceSet) {
        assert resourceSet

        StringBuilder sb = new StringBuilder()

        if (resourceSet.isMemSet()) sb << " -l mem=" << resourceSet.getMem().toString(BufferUnit.M)
        if (resourceSet.isWalltimeSet()) sb << " -l walltime=" << resourceSet.getWalltime()
        if (job?.customQueue) sb << " -q " << job.customQueue
        else if (resourceSet.isQueueSet()) sb << " -q " << resourceSet.getQueue()

        if (resourceSet.isCoresSet() || resourceSet.isNodesSet()) {
            int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
            int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1
            // Currently not active
            String enforceSubmissionNodes = ""
            // configuration.getConfigurationValues().getString(CVALUE_ENFORCE_SUBMISSION_TO_NODES, null)
            if (!enforceSubmissionNodes) {
                sb << " -l nodes=" << nodes << ":ppn=" << cores
                if (resourceSet.isAdditionalNodeFlagSet()) {
                    sb << ":" << resourceSet.getAdditionalNodeFlag()
                }
            } else {
                String[] nodesArr = enforceSubmissionNodes.split(StringConstants.SPLIT_SEMICOLON)
                nodesArr.each {
                    String node ->
                        sb << " -l nodes=" << node << ":ppn=" << resourceSet.getCores()
                }
            }
        }

        if (resourceSet.isStorageSet()) {
//            sb << " -l mem=" << resourceSet.getMem() << "g");
        }

        return new PBSResourceProcessingCommand(sb.toString())
    }

    String getResourceOptionsPrefix() {
        return "PBSResourceOptions_"
    }

    static ProcessingCommands convertPBSResourceOptionsString(String processingString) {
        return new PBSResourceProcessingCommand(processingString)
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
    ProcessingCommands extractProcessingCommandsFromToolScript(File file) {
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
        return convertPBSResourceOptionsString(processingOptionsStr.toString())
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
//        return null
        GenericJobInfo jInfo = parseGenericJobInfo(commandString)
        BEJob job = new BEJob(jInfo.getJobName(), jInfo.getTool(), null, "", null, [], jInfo.getParameters(), null, jInfo.getParentJobIDs().collect {
            new PBSJobDependencyID(null, it)
        } as List<de.dkfz.roddy.execution.jobs.JobDependencyID>, this);

        //Autmatically get the status of the job and if it is planned or running add it as a job status listener.
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
    JobResult convertToArrayResult(BEJob arrayChildJob, JobResult parentJobsResult, int arrayIndex) {
        return null
    }

    private static final ReentrantLock cacheLock = new ReentrantLock()

    private static ExecutionResult cachedExecutionResult

    protected Map<String, JobState> allStates = [:]

    protected Map<String, BEJob> jobStatusListeners = [:]

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


        Map<String, JobState> allStatesTemp = [:]
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

        if (er.successful) {
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
                    if (logger.isVerbosityHigh()) {
                        System.out.println("QStat BEJob line: " + line)
                        System.out.println("	Entry in arr[" + ID + "]: " + split[ID])
                        System.out.println("    Entry in arr[" + JOBSTATE + "]: " + split[JOBSTATE])
                    }

                    String[] idSplit = split[ID].split("[.]")
                    //(idSplit.length <= 1) continue;
                    String id = idSplit[0]
                    JobState js = JobState.UNKNOWN
                    if (split[JOBSTATE] == getStringForRunningJob())
                        js = JobState.RUNNING
                    if (split[JOBSTATE] == getStringForJobOnHold())
                        js = JobState.HOLD
                    if (split[JOBSTATE] == getStringForQueuedJob())
                        js = JobState.QUEUED
                    if (split[JOBSTATE] == getStringForCompletedJob()) {
                        js = JobState.COMPLETED_UNKNOWN
                    }

                    allStatesTemp.put(id, js)
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
                    JobState js = allStatesTemp.get(id)
                    BEJob job = jobStatusListeners.get(id)

                    if (js == JobState.UNKNOWN_SUBMITTED) {
                        // If the jobState is unknown and the job is not running anymore it is counted as failed.
                        job.setJobState(JobState.FAILED)
                        removejobs.add(job)
                        continue
                    }

                    if (JobState._isPlannedOrRunning(js)) {
                        job.setJobState(js)
                        continue
                    }

                    if (job.getJobState() == JobState.OK || job.getJobState() == JobState.FAILED)
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

    @Override
    String getStringForRunningJob() {
        return PBS_JOBSTATE_RUNNING
    }

//    @Override
    String getStringForCompletedJob() {
        return PBS_JOBSTATE_COMPLETED_UNKNOWN
    }

    @Override
    String getSpecificJobIDIdentifier() {
        return PBS_JOBID
    }

    @Override
    String getSpecificJobArrayIndexIdentifier() {
        return PBS_ARRAYID
    }

    @Override
    String getSpecificJobScratchIdentifier() {
        return PBS_SCRATCH
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
    Map<String, JobState> queryJobStatusById(List<String> jobIds, boolean forceUpdate = false) {

        if (allStates == null || forceUpdate)
            updateJobStatus(forceUpdate)

        Map<String, JobState> queriedStates = jobIds.collectEntries { String jobId -> [jobId, JobState.UNKNOWN] }

        for (String jobId in jobIds) {
            // Aborted somewhat supercedes everything.
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
    //Map<BEJob, GenericJobInfo>
    Map<BEJob, GenericJobInfo> queryExtendedJobState(List<BEJob> jobs, boolean forceUpdate) {

        Map<String, GenericJobInfo> queriedExtendedStates = queryExtendedJobStateById(jobs.collect {
            it.getJobID()
        }, false)
        return (Map<BEJob, GenericJobInfo>) queriedExtendedStates.collectEntries { Entry<String, GenericJobInfo> it -> [jobs.find { BEJob temp -> temp.getJobID() == it.key }, (GenericJobInfo) it.value] }
    }

    @Override
    //Map<String, GenericJobInfo>
    Map<String, GenericJobInfo> queryExtendedJobStateById(List<String> jobIds, boolean forceUpdate) {
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
            queriedExtendedStates = queryJobState(er.resultLines)
        }
        return queriedExtendedStates
    }

    Map<String, GenericJobInfo> queryJobState(List<String> resultLines) {
        Map<String, GenericJobInfo> queriedExtendedStates = [:]

        Map<String, Map<String, String>> qstatReaderResult = PBSQstatReader.conv(resultLines.toString())
        qstatReaderResult.each { it ->

            Map<String, String> jobResult = it.getValue()
            GenericJobInfo gj = new GenericJobInfo(jobResult.get("Job_Name"), null, it.getKey(), null, jobResult.get("depend") ? jobResult.get("depend").find("afterok.*")?.findAll(/(\d+).(\w+)/) { fullMatch, beforeDot, afterDot -> return beforeDot } : null)

            BufferValue mem = null
            int cores
            int nodes
            TimeUnit walltime = null
            if (jobResult.get("Resource_List.mem"))
                mem = new BufferValue(Integer.valueOf(jobResult.get("Resource_List.mem").find(/(\d+)/)), BufferUnit.valueOf(jobResult.get("Resource_List.mem")[-2]))
            if (jobResult.get("Resource_List.nodect"))
                cores = Integer.valueOf(jobResult.get("Resource_List.nodect"))
            if (jobResult.get("Resource_List.nodes"))
                nodes = Integer.valueOf(jobResult.get("Resource_List.nodes").find(/(\d+)/))
            if (jobResult.get("Resource_List.walltime"))
                walltime = new TimeUnit(jobResult.get("Resource_List.walltime"))

            BufferValue usedMem = null
            TimeUnit usedWalltime = null
            if (jobResult.get("resources_used.mem"))
                usedMem = new BufferValue(Integer.valueOf(jobResult.get("resources_used.mem").find(/(\d+)/)), BufferUnit.valueOf(jobResult.get("resources_used.mem")[-2]))
            if (jobResult.get("resources_used.walltime"))
                usedWalltime = new TimeUnit(jobResult.get("resources_used.walltime"))

            gj.setAskedResources(new ResourceSet(null, mem, cores, nodes, walltime, null, jobResult.get("queue"), null))
            gj.setUsedResources(new ResourceSet(null, usedMem, null, null, usedWalltime, null, jobResult.get("queue"), null))

            gj.setOutFile(jobResult.get("Output_Path"))
            gj.setErrorFile(jobResult.get("Error_Path"))
            gj.setUser(jobResult.get("euser"))
            gj.setExHosts(jobResult.get("exec_host"))
            gj.setSubHost(jobResult.get("submit_host"))
            gj.setPriority(jobResult.get("Priority"))
            gj.setUserGroup(jobResult.get("egroup"))
            gj.setResReq(jobResult.get("submit_args"))
            gj.setRunTime(jobResult.get("total_runtime") ? Duration.ofSeconds(Math.round(Double.parseDouble(jobResult.get("total_runtime"))), 0) : null)
            gj.setCpuTime(jobResult.get("resources_used.cput") ? Duration.parse("PT" + jobResult.get("resources_used.cput").substring(0, 2) + "H" + jobResult.get("resources_used.cput").substring(3, 5) + "M" + jobResult.get("resources_used.cput").substring(6) + "S") : null)
            gj.setServer(jobResult.get("server"))
            gj.setUmask(jobResult.get("umask"))

            DateTimeFormatter pbsDatePattern = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss yyyy").withLocale(Locale.ENGLISH)
            if (jobResult.get("qtime"))
                gj.setSubTime(LocalDateTime.parse(jobResult.get("qtime"), pbsDatePattern))
            if (jobResult.get("start_time"))
                gj.setStartTime(LocalDateTime.parse(jobResult.get("start_time"), pbsDatePattern))
            if (jobResult.get("comp_time"))
                gj.setEndTime(LocalDateTime.parse(jobResult.get("comp_time"), pbsDatePattern))

            queriedExtendedStates.put(it.getKey(), gj)
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
            throw new RuntimeException("Abortion of job states failed.")
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
    File getLoggingDirectoryForJob(BEJob job) {
        logger.severe("We do not know yet, how to query the default logging directory... the submission server does not necessarily have to know about this.")
        logger.severe("We assume, that the logging directory is set to the current working directory automatically or to the home folder.")
        return executionService.queryWorkingDirectory()
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
        String cmd = String.format("jobHost=`qstat -f %s  | grep exec_host | cut -d \"/\" -f 1 | cut -d \"=\" -f 2`; ssh %s@${jobHost: 1} 'cat /opt/torque/spool/spool/*'%s'*'", id, user, searchID)
        ExecutionResult executionResult = executionService.execute(cmd)
        if (executionResult.successful)
            return executionResult.resultLines.toArray(new String[0])
        return new String[0]
    }

    @Override
    String parseJobID(String commandOutput) {
        return commandOutput
    }

    @Override
    String getSubmissionCommand() {
        return PBSCommand.QSUB
    }

}
