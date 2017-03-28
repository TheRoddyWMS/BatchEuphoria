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
    public static final String PBS_COMMAND_QUERY_STATES = "qstat -t"
    public static final String PBS_COMMAND_DELETE_JOBS = "qdel"
    public static final String PBS_LOGFILE_WILDCARD = "*.o"
    public static final String PBS_JOBID = '${PBS_JOBID}'
    public static final String PBS_ARRAYID = '${PBS_ARRAYID}'
    public static final String PBS_SCRATCH = '${PBS_SCRATCH_DIR}/${PBS_JOBID}'

    private Map<String, Boolean> mapOfInitialQueries = new LinkedHashMap<>()

    PBSJobManager(ExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms)
    }
// Will not work in first implementation. This constructor was used in the transformation process from one Batch system to another one (e.g. PBS => SGE)
//    @Override
//    public PBSCommand createCommand(GenericJobInfo jobInfo) {
//        Job job = new Job(jobInfo.getExecutionContext(), jobInfo.getJobName(), jobInfo.getToolID(), new LinkedHashMap<String, Object>(jobInfo.getParameters()));
//        PBSCommand pbsCommand = new PBSCommand(job, jobInfo.getExecutionContext(), ExecutionService.getInstance(), job.getJobName(), null, job.getParameters(), null, jobInfo.getParentJobIDs(), jobInfo.getExecutionContext().getConfiguration().getProcessingToolPath(jobInfo.getExecutionContext(), jobInfo.getToolID()).getAbsolutePath());
//        return pbsCommand;
//    }

    @Override
    PBSCommand createCommand(GenericJobInfo jobInfo) {
        throw new NotImplementedException()
    }

//    @Override
    PBSCommand createCommand(Job job, List<ProcessingCommands> processingCommands, String command, Map<String, String> parameters,  Map<String, Object> tags, List<String> dependencies, List<String> arraySettings, File logDirectory) {
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
        executionService.execute(command)
        // job.runResult is set within executionService.execute
        logger.severe("Set the job runResult in a better way from runJob itself or so.")
        return job.runResult
    }

    @Override
    JobResult runJob(Job job, boolean runDummy) {
//        createCommand(job, job.jobName, null, job.tool, job.parameters, job.dependencyIDs, )
//        executionService.execute()
        throw new NotImplementedException()
    }

    /**
     * For BPS, we enable hold jobs by default.
     * If it is not enabled, we might run into the problem, that job dependencies cannot be
     * ressolved early enough due to timing problems.
     * @return
     */
    @Override
    boolean getDefaultForHoldJobsEnabled() { return true; }

    @Override
    void startHeldJobs(List<Job> heldJobs) {
        if (!isHoldJobsEnabled()) return
        if (!heldJobs) return
        String qrls = "qrls ${heldJobs.collect { it.runResult?.jobID?.shortID }.findAll { it}.join(" ")}"
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
        if (resourceSet.isWalltimeSet()) sb.append(" -l walltime=").append(resourceSet.getWalltime())
        if (resourceSet.isQueueSet()) sb.append(" -q ").append(resourceSet.getQueue())

        if (resourceSet.isCoresSet() || resourceSet.isNodesSet()) {
            int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
            int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1
            // Currently not active
            String enforceSubmissionNodes = "" // configuration.getConfigurationValues().getString(CVALUE_ENFORCE_SUBMISSION_TO_NODES, null)
            if (!enforceSubmissionNodes) {
                sb.append(" -l nodes=").append(nodes).append(":ppn=").append(cores)
                if (resourceSet.isAdditionalNodeFlagSet()) {
                    sb.append(":").append(resourceSet.getAdditionalNodeFlag())
                }
            } else {
                String[] nodesArr = enforceSubmissionNodes.split(StringConstants.SPLIT_SEMICOLON)
                nodesArr.each {
                    String node ->
                        sb.append(" -l nodes=").append(node).append(":ppn=").append(resourceSet.getCores())
                }
            }
        }

        logger.severe("Allow enabling and disabling of options for resource arbitration for defective job managers.")
        if (resourceSet.isStorageSet()) {
//            sb.append(" -l mem=").append(resourceSet.getMem()).append("g");
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
            processingOptionsStr.append(" ").append(line.substring(5))
        }
        return convertPBSResourceOptionsString(processingOptionsStr.toString())
    }

    @Override
    Job parseToJob(String commandString) {
        throw new NotImplementedException()
    }

//    @Override
//    Job parseToJob(ExecutionContext executionContext, String commandString) {
//
//        GenericJobInfo jInfo = parseGenericJobInfo(executionContext, commandString)
//        Job job = new ReadOutJob(executionContext, jInfo.getJobName(), jInfo.getToolID(), jInfo.getID(), jInfo.getParameters(), jInfo.getParentJobIDs())
//
//        //Autmatically get the status of the job and if it is planned or running add it as a job status listener.
//        String shortID = job.getJobID()
//        job.setJobState(queryJobStatus(Arrays.asList(shortID)).get(shortID))
//        if (job.getJobState().isPlannedOrRunning()) addJobStatusChangeListener(job)
//
//        return job
//    }

    @Override
    GenericJobInfo parseGenericJobInfo(String commandString) {
        throw new NotImplementedException();
    }

    @Override
    JobResult convertToArrayResult(Job arrayChildJob, JobResult parentJobsResult, int arrayIndex) {
        return null
    }

//        commandString = commandString.trim()
//        String[] split = commandString.split(", ")
//        String id = split[0]
//        commandString = split[1].trim()
//
//        if (!commandString.startsWith("qsub")) return null
//        int skriptIndex = -1
//        final String DBL_QUOTE = "\""
//        if (commandString.endsWith(DBL_QUOTE)) {
//            skriptIndex = commandString[0..-2].lastIndexOf(DBL_QUOTE)
//        } else {
//            skriptIndex = commandString.lastIndexOf(" ")
//        }
//
//        String script = commandString[skriptIndex..-1].trim()
//        commandString = commandString[5..skriptIndex].trim()
//        String[] options = (" " + commandString).split(" [-]")   //Put " " in front of the string so that every command can be recognized properly beginning with the first one.
//        String jobName = "not readable"
//        String toolID = ""
//        String walltime
//        String memory
//        BufferUnit bufferUnit = BufferUnit.G
//        String cores
//        String nodes
//        String queue
//        String otherSettings
//        List<String> dependencies = new LinkedList<>()
//
//        Map<String, String> parameters = new LinkedHashMap<>()
//        for (String option : options) {
//            if (option.trim().length() == 0) continue //Sometimes empty options occur.
//            String var = option.substring(0, 1)
//            String parms = option.substring(1).trim()
//            if (var.equals("N")) {
//                jobName = parms
//                try {
//                    toolID = jobName.substring(jobName.lastIndexOf("_") + 1)
//                } catch (Exception e) {
//                    //TODO Jobnames not read out properly in some cases
//                    e.printStackTrace()
//                }
//            } else if (var.equals("v")) {
//                String[] variables = parms.split(SPLIT_COMMA)
//                for (String variable : variables) {
//                    String[] varSplit = variable.split(SPLIT_EQUALS)
//                    String header = varSplit[0]
//                    String value = Constants.UNKNOWN
//                    if (varSplit.length > 1)
//                        value = varSplit[1]
//                    parameters.put(header, value)
//                }
//            } else if (var.equals("l")) { //others
//                def splitParms = parms.split("[,]")
//                splitParms.each {
//                    String parm ->
//                        String parmID = parm.split(StringConstants.SPLIT_EQUALS)[0]
//                        String parmVal = parm.split(StringConstants.SPLIT_EQUALS)[1]
//                        if (parmID == "mem") {
//                            bufferUnit = BufferUnit.valueOf(parmVal[-1])
//                            memory = parmVal[0..-2]
//                        } else if (parmID == "walltime") {
//                            String[] splitParm = parmVal.split(StringConstants.SPLIT_COLON)
//                            //TODO Increase overall granularity for walltime and mem in config.
//                        } else if (parmID == "nodes") {
//                            String[] splitParm = parmVal.split(StringConstants.SPLIT_COLON)
//                            nodes = splitParm[0]
//                            cores = splitParm[1]
//                        }
//                }
//            } else if (var.equals("W")) {
//                if (parms.startsWith("depend")) {
//                    def deps = parms[7..-1].split("[:]")
////                    println deps;
//                    if (deps[0] != "afterok")
//                        println "Not supported: " + deps[0]
////                    println deps[1 .. -1]
//                    try {
//                        dependencies.addAll(deps[1..-1])
//                    } catch (Exception ex) {
//                        println(parms)
//                        println(ex)
//                    }
//                }
//            }
//        }
//
//        GenericJobInfo jInfo = new GenericJobInfo(executionContext, jobName, toolID, id, parameters, dependencies)
//        jInfo.setCpus(cores)
//        jInfo.setNodes(nodes)
//        jInfo.setMemory(memory)
//        jInfo.setMemoryBufferUnit(bufferUnit)
//        jInfo.setWalltime(walltime)
////        jInfo.set
//        return jInfo
//    }

//    @Override
//    JobResult convertToArrayResult(Job arrayChildJob, JobResult parentJobsResult, int arrayIndex) {
//        String newID
//        if (parentJobsResult.getJobID() instanceof JobDependencyID.FakeJobID) {
//            newID = parentJobsResult.getJobID().getId() + "[" + arrayIndex + "]"
//        } else {
//            String parentID = (parentJobsResult.getJobID()).getId()
//            String[] split = parentID.split("\\[")
//            newID = split[0] + "[" + arrayIndex + split[1]
//        }
//        PBSJobDependencyID pjid = new PBSJobDependencyID(arrayChildJob, newID)
//        return new JobResult(parentJobsResult.getRun(), parentJobsResult.getCommand(), pjid, parentJobsResult.isWasExecuted(), false, parentJobsResult.getToolID(), parentJobsResult.getJobParameters(), parentJobsResult.getParentJobs())
//    }

    private static final ReentrantLock cacheLock = new ReentrantLock()

    private static ExecutionResult cachedExecutionResult

    protected Map<String, JobState> allStates = new LinkedHashMap<String, JobState>()

    protected Map<String, Job> jobStatusListeners = new LinkedHashMap<>()

    /**
     * Queries the jobs states.
     *
     * @return
     */
    public void updateJobStatus() {
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


        Map<String, JobState> allStatesTemp = new LinkedHashMap<String, JobState>()
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
                    if (split[JOBSTATE].equals(getStringForRunningJob()))
                        js = JobState.RUNNING
                    if (split[JOBSTATE].equals(getStringForJobOnHold()))
                        js = JobState.HOLD
                    if (split[JOBSTATE].equals(getStringForQueuedJob()))
                        js = JobState.QUEUED

                    allStatesTemp.put(id, js)
                    if (logger.isVerbosityHigh())
                        System.out.println("   Extracted jobState: " + js.toString())
                }
            }

            logger.severe("Reading out job states from job state logfiles is not possible yet!")

            // I don't currently know, if the jslisteners are used.
//            //Create a local cache of jobstate logfile entries.
//            Map<ExecutionContext, Map<String, JobState>> map = new LinkedHashMap<>()
//            List<Job> removejobs = new LinkedList<>()
//            synchronized (jobStatusListeners) {
//                for (String id : jobStatusListeners.keySet()) {
//                    JobState js = allStatesTemp.get(id)
//                    Job job = jobStatusListeners.get(id)
//
//                    if (js == JobState.UNKNOWN_SUBMITTED) {
//                        // If the jobState is unknown and the job is not running anymore it is counted as failed.
//                        job.setJobState(JobState.FAILED)
//                        removejobs.add(job)
//                        continue
//                    }
//
//                    if (JobState._isPlannedOrRunning(js)) {
//                        job.setJobState(js)
//                        continue
//                    }
//
//                    if (job.getJobState() == JobState.OK || job.getJobState() == JobState.FAILED)
//                        continue //Do not query jobs again if their status is already final. TODO Remove final jobs from listener list?
//
//                    if (js == null || js == JobState.UNKNOWN) {
//                        //Read from jobstate logfile.
//                        try {
//                            ExecutionContext executionContext = job.getExecutionContext()
//                            if (!map.containsKey(executionContext))
//                                map.put(executionContext, executionContext.readJobStateLogFile())
//
//                            JobState jobsCurrentState = null
//                            Map<String, JobState> statesMap = map.get(executionContext)
//                            if (job.getRunResult() != null) {
//                                jobsCurrentState = statesMap.get(job.getRunResult().getJobID().getId())
//                            } else { //Search within entries.
//                                for (String s : statesMap.keySet()) {
//                                    if (s.startsWith(id)) {
//                                        jobsCurrentState = statesMap.get(s)
//                                        break
//                                    }
//                                }
//                            }
//                            js = jobsCurrentState
//                        } catch (Exception ex) {
//                            //Could not read out job jobState from file
//                        }
//                    }
//                    job.setJobState(js)
//                }
//            }
        }
        cacheLock.lock()
        allStates.clear()
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

    Map<String, JobState> queryJobStatus(List<String> jobIDs) {
        return queryJobStatus(jobIDs, false)
    }

    Map<String, JobState> queryJobStatus(List<String> jobIDs, boolean forceUpdate) {

        if (allStates == null || forceUpdate)
            updateJobStatus(forceUpdate)

        Map<String, JobState> queriedStates = new LinkedHashMap<String, JobState>()

        for (String id : jobIDs) {
            cacheLock.lock()
            JobState state = allStates.get(id)
            cacheLock.unlock()
            queriedStates.put(id, state)
        }

        return queriedStates
    }

    @Override
    void queryJobAbortion(List<Job> executedJobs) {
        StringBuilder queryCommand = new StringBuilder(PBS_COMMAND_DELETE_JOBS)

        for (Job job : executedJobs)
            queryCommand.append(" ").append(job.getJobID())

        executionService.execute(queryCommand.toString(), false)
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
