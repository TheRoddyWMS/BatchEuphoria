/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.execution.cluster.pbs

import de.dkfz.eilslabs.batcheuphoria.config.ResourceSet
import de.dkfz.eilslabs.batcheuphoria.execution.ExecutionService
import de.dkfz.eilslabs.batcheuphoria.execution.cluster.ClusterJobManager
import de.dkfz.eilslabs.batcheuphoria.jobs.*
import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.JobResult
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.LoggerWrapper
import de.dkfz.roddy.tools.RoddyConversionHelperMethods
import de.dkfz.roddy.tools.RoddyIOHelperMethods
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import java.util.concurrent.locks.ReentrantLock

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
    public static final String PBS_COMMAND_DELETE_JOBS = "qdel"
    public static final String PBS_LOGFILE_WILDCARD = "*.o"
    public static final String PBS_JOBID = '${PBS_JOBID}'
    public static final String PBS_ARRAYID = '${PBS_ARRAYID}'
    public static final String PBS_SCRATCH = '${PBS_SCRATCH_DIR}/${PBS_JOBID}'

    private Map<String, Boolean> mapOfInitialQueries = new LinkedHashMap<>()

    PBSJobManager(ExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms)
        /**
         * General or specific todos for JobManager and PBSJobManager
         */
        logger.severe("Need to find a way to properly get the job state for a completed job. Neither tracejob, nor qstat -f are a good way. qstat -f only works for 'active' jobs. Lists with long active lists are not default.")
        logger.severe("Set logfile location, parameter file and job state log file on job creation (or override a method).")
        logger.severe("Allow enabling and disabling of options for resource arbitration for defective job managers.")
        logger.severe("parseToJob() is not implemented and will return null.")
    }
// Will not work in first implementation. This constructor was used in the transformation process from one Batch system to another one (e.g. PBS => SGE)
//    @Override
//    PBSCommand createCommand(GenericJobInfo jobInfo) {
//        Job job = new Job(jobInfo.getJobName(), jobInfo.getTool(), new LinkedHashMap<String, Object>(jobInfo.getParameters()));
//        PBSCommand pbsCommand = new PBSCommand(job, jobInfo.getExecutionContext(), ExecutionService.getInstance(), job.getJobName(), null, job.getParameters(), null, jobInfo.getParentJobIDs(), jobInfo.getExecutionContext().getConfiguration().getProcessingToolPath(jobInfo.getExecutionContext(), jobInfo.getTool()).getAbsolutePath());
//        return pbsCommand;
//    }

    @Override
    PBSCommand createCommand(GenericJobInfo jobInfo) {
        throw new NotImplementedException()
    }

//    @Override
    PBSCommand createCommand(Job job, List<ProcessingCommands> processingCommands, String command, Map<String, String> parameters, Map<String, Object> tags, List<String> dependencies, List<String> arraySettings, File logDirectory) {
        PBSCommand pbsCommand = new PBSCommand(this, job, job.jobID, processingCommands, parameters, tags, arraySettings, dependencies, command, logDirectory)
        return pbsCommand
    }

    @Override
    PBSCommand createCommand(Job job, String jobName, List<ProcessingCommands> processingCommands, File tool, Map<String, String> parameters, List<String> dependencies, List<String> arraySettings) {
//        PBSCommand pbsCommand = new PBSCommand(this, job, job.jobID, processingCommands, parameters, tags, arraySettings, dependencies, command, logDirectory)
//        return pbsCommand
        throw new NotImplementedException()
    }

    PBSCommand createCommand(Job job) {
        return new PBSCommand(this, job, job.jobName, [], job.parameters, [:], [], job.dependencyIDsAsString, job.tool.getAbsolutePath(), job.getLoggingDirectory())
    }

    @Override
    JobResult runJob(Job job) {
        def command = createCommand(job)
        def executionResult = executionService.execute(command)
        extractAndSetJobResultFromExecutionResult(command, executionResult)
        executionService.handleServiceBasedJobExitStatus(command, executionResult, null)
        // job.runResult is set within executionService.execute
//        logger.severe("Set the job runResult in a better way from runJob itself or so.")
        cacheLock.lock()
        if (job.runResult.wasExecuted && job.jobManager.isHoldJobsEnabled()) {
            allStates[job.jobID] = JobState.HOLD
        } else if (job.runResult.wasExecuted) {
            allStates[job.jobID] = JobState.QUEUED
        } else {
            allStates[job.jobID] = JobState.FAILED
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

    List<String> collectJobIDsFromJobs(List<Job> jobs) {
        jobs.collect { it.runResult?.jobID?.shortID }.findAll { it }
    }

    @Override
    void startHeldJobs(List<Job> heldJobs) {
        if (!isHoldJobsEnabled()) return
        if (!heldJobs) return
        String qrls = "qrls ${collectJobIDsFromJobs(heldJobs).join(" ")}"
        executionService.execute(qrls)
    }

    @Override
    JobDependencyID createJobDependencyID(Job job, String jobResult) {
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
    ProcessingCommands convertResourceSet(ResourceSet resourceSet) {
        StringBuilder sb = new StringBuilder()

        if (resourceSet.isMemSet()) sb << " -l mem=" << resourceSet.getMem().toString(BufferUnit.M)
        if (resourceSet.isWalltimeSet()) sb << " -l walltime=" << resourceSet.getWalltime()
        if (resourceSet.isQueueSet()) sb << " -q " << resourceSet.getQueue()

        if (resourceSet.isCoresSet() || resourceSet.isNodesSet()) {
            int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
            int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1
            // Currently not active
            String enforceSubmissionNodes = "" // configuration.getConfigurationValues().getString(CVALUE_ENFORCE_SUBMISSION_TO_NODES, null)
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
    Job parseToJob(String commandString) {
//        return null
        GenericJobInfo jInfo = parseGenericJobInfo(commandString)
        Job job = new Job(jInfo.getJobName(), jInfo.getTool(),null, "", null, [], jInfo.getParameters(), null, jInfo.getParentJobIDs().collect { new PBSJobDependencyID(null, it) } as List<de.dkfz.roddy.execution.jobs.JobDependencyID>, this);

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
    JobResult convertToArrayResult(Job arrayChildJob, JobResult parentJobsResult, int arrayIndex) {
        return null
    }

    private static final ReentrantLock cacheLock = new ReentrantLock()

    private static ExecutionResult cachedExecutionResult

    protected Map<String, JobState> allStates = [:]

    protected Map<String, Job> jobStatusListeners = [:]

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
        }
        er = cachedExecutionResult
        resultLines.addAll(er.resultLines)

        cacheLock.unlock()

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
                        System.out.println("QStat Job line: " + line)
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
            List<Job> removejobs = new LinkedList<>()
            synchronized (jobStatusListeners) {
                for (String id : jobStatusListeners.keySet()) {
                    JobState js = allStatesTemp.get(id)
                    Job job = jobStatusListeners.get(id)

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
                        continue //Do not query jobs again if their status is already final. TODO Remove final jobs from listener list?

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
//            allStates[allStates.find { Job job, JobState state -> job.jobID == id }?.key] = status
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
    Map<Job, JobState> queryJobStatus(List<Job> jobs, boolean forceUpdate = false) {

        if (allStates == null || forceUpdate)
            updateJobStatus(forceUpdate)

        Map<Job, JobState> queriedStates = jobs.collectEntries { Job job -> [job, JobState.UNKNOWN] }

        for (Job job in jobs) {
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
    Map<Job, GenericJobInfo> queryExtendedJobState(List<Job> jobs, boolean forceUpdate) {
        return null
    }

    @Override
    void queryJobAbortion(List<Job> executedJobs) {
        def executionResult = executionService.execute("${PBS_COMMAND_DELETE_JOBS} ${collectJobIDsFromJobs(executedJobs).join(" ")}", false)
        if (executionResult.successful) {
            executedJobs.each { Job job -> job.jobState = JobState.ABORTED }
        } else {
            logger.always("Need to create a proper fail message for abortion.")
            throw new RuntimeException("Abortion of job states failed.")
        }
    }

    @Override
    void addJobStatusChangeListener(Job job) {
        synchronized (jobStatusListeners) {
            jobStatusListeners.put(job.getJobID(), job)
        }
    }

    @Override
    String getLogFileWildcard(Job job) {
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
    String[] peekLogFile(Job job) {
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
