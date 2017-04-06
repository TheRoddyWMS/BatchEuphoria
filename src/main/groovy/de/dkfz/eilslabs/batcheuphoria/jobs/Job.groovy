/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs

import de.dkfz.eilslabs.batcheuphoria.config.ResourceSet
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import java.util.concurrent.atomic.AtomicLong

/**
 * The job class represents a generic and abstract form of cluster job which can be run using a job manager.
 * When a job is executed with the JM, the used Command and a JobResult object will be created and added to this
 * object.
 */
@groovy.transform.CompileStatic
class Job<J extends Job> {

    private static final de.dkfz.roddy.tools.LoggerWrapper logger = de.dkfz.roddy.tools.LoggerWrapper.getLogger(Job.class.getSimpleName())

    protected JobType jobType = JobType.STANDARD

    private final List<Job> arrayChildJobs = new LinkedList<>()

    private static AtomicLong absoluteJobCreationCounter = new AtomicLong()

    /**
     * The name of the job which should be passed to the execution system.
     */
    public final String jobName

    /**
     * An internal job creation count. Has nothing to do with e.g. PBS / cluster / process id's!
     */
    public final long jobCreationCounter = absoluteJobCreationCounter.incrementAndGet()

    /**
     * The tool you want to call.
     */
    protected File tool

    /**
     * MD5 sum of the called tool
     */
    protected String toolMD5

    /**
     * A tool script which will be piped (or whatever...) to the job manager submission command / method
     * It is either toolScript OR tool
     */
    String toolScript
    /**
     * The set of resources for this tool / Job
     * Contains values for e.g. memory, walltime and so on.
     */
    protected ResourceSet resourceSet

    /**
     * Parameters for the tool you want to call
     */
    protected final Map<String, String> parameters

    /**
     * If you want to generated arrays use this. <p>You can do things like: n-m,
     */
    protected final List<String> arrayIndices

    protected final List<J> parentJobs

    /**
     * You should provide i.e. job ids of qsub jobs to automatically create job
     * dependencies.
     */
    protected final List<de.dkfz.roddy.execution.jobs.JobDependencyID> listOfCustomDependencyIDs = new LinkedList<>()

    /**
     * Temporary value which defines the jobs jobState.
     */
    protected transient JobState currentJobState

    private List<ProcessingCommands> processingCommand = new LinkedList<ProcessingCommands>()

    /**
     * Command object of last execution.
     */
    protected transient Command lastCommand

    /**
     * Stores the result when the job was executed.
     */
    private de.dkfz.roddy.execution.jobs.JobResult runResult

    private GenericJobInfo jobInfo

    JobManager jobManager

    Job(String jobName, File tool, String toolScript, String toolMD5, ResourceSet resourceSet, List<String> arrayIndices, Map<String, String> parameters, List<Job> parentJobs, List<de.dkfz.roddy.execution.jobs.JobDependencyID> dependencyIDs, JobManager jobManager) {
        this.jobName = jobName
        this.currentJobState = JobState.UNKNOWN
        this.tool = tool
        this.toolScript = toolScript
        if( tool && toolScript ) throw new RuntimeException("A job must only have an input script or a callable file.")
        this.toolMD5 = toolMD5
        this.resourceSet = resourceSet
        this.parameters = parameters
        this.parentJobs = parentJobs
        this.arrayIndices = arrayIndices ?: new LinkedList<String>()
        this.listOfCustomDependencyIDs.addAll((dependencyIDs ?: parentJobs ? parentJobs.collect { Job job -> job.runResult?.jobID ?: null }.findAll { de.dkfz.roddy.execution.jobs.JobDependencyID it -> it } : []) as List<JobDependencyID>)
        this.jobManager = jobManager
    }

    protected void setJobType(JobType jobType) {
        this.jobType = jobType
    }

    //TODO Create a runArray method which returns several job results with proper array ids.
    de.dkfz.roddy.execution.jobs.JobResult run() {

    }

    boolean isFakeJob() {
        if (this instanceof FakeJob)
            return true
        if (jobName != null && jobName.equals("Fakejob"))
            return true
        String jobID = getJobID()
        if (jobID == null)
            return false
        return FakeJobID.isFakeJobID(jobID)
    }

    protected void postProcessArrayJob(JobResult runResult) {
        throw new NotImplementedException()
        Map<String, Object> prmsAsStringMap = new LinkedHashMap<>()
        for (String k : parameters.keySet()) {
            prmsAsStringMap.put(k, parameters.get(k))
        }
        jobType = JobType.ARRAY_HEAD
        //TODO Think of proper array index handling!
        int i = 1
    }

    void addProcessingCommand(ProcessingCommands processingCommand) {
        if (processingCommand == null) return
        this.processingCommand.add(processingCommand)
    }

    JobType getJobType() {
        return jobType
    }

    Map<String, String> getParameters() {
        return parameters
    }

    List<String> finalParameters() {
        return parameters.collect { String k, String v -> return "${k}=${v}".toString() }
    }

    List<ProcessingCommands> getListOfProcessingCommand() {
        return [jobManager.convertResourceSet(resourceSet)] + processingCommand
    }

    List<J> getParentJobs() {
        return parentJobs
    }

    List<de.dkfz.roddy.execution.jobs.JobDependencyID> getDependencyIDs() {
        if (listOfCustomDependencyIDs)
            return listOfCustomDependencyIDs

        def res = getParentJobs()?.collect { ((Job) it).runResult?.jobID }?.findAll { it }?.unique()
        if (!res) return []
        return res
    }

    List<String> getDependencyIDsAsString() {
        def depIDs = getDependencyIDs()
        def res = depIDs.collect { de.dkfz.roddy.execution.jobs.JobDependencyID jid -> jid.id }
        return res as List<String>
    }

    File getLoggingDirectory() {

    }

    void setRunResult(de.dkfz.roddy.execution.jobs.JobResult result) {
        this.runResult = result
    }

    de.dkfz.roddy.execution.jobs.JobResult getRunResult() {
        return runResult
    }


    GenericJobInfo getJobInfo() {
        return jobInfo
    }

    void setJobInfo(GenericJobInfo jobInfo) {
        this.jobInfo = jobInfo
    }

    /**
     * If the job was executed this return the jobs id otherwise null.
     *
     * @return
     */
    String getJobID() {
        if (runResult != null)
            if (runResult.getJobID() != null)
                return runResult.getJobID().getShortID()
            else
                return "Unknown"
        else
            return null
    }

//    String getToolID() {
//        return toolID
//    }

    File getTool() {
        return tool
    }

    String getToolMD5() {
        return toolMD5
    }

    ResourceSet getResourceSet() {
        return resourceSet
    }

    protected File _logFile = null
    /**
     * Returns the path to an existing log file.
     * If no logfile exists this returns null.
     *
     * @return
     */
    public synchronized File getLogFile() {
//        if (_logFile == null)
//            _logFile = this.getExecutionContext().getRuntimeService().getLogFileForJob(this);
//        return _logFile;
    }

    public boolean hasLogFile() {
//        if (getJobState().isPlannedOrRunning())
//            return false;
//        if (_logFile == null)
//            return this.getExecutionContext().getRuntimeService().hasLogFileForJob(this);
//        return true;
        return false
    }

    String getJobName() {
        return jobName
    }

    File getParameterFile() {
        return null
    }

    void setJobState(JobState js) {
        if (jobType == JobType.ARRAY_HEAD)
            return
        JobState old = this.currentJobState
        this.currentJobState = js
    }

    JobState getJobState() {
        if (jobType == JobType.ARRAY_HEAD) {
            int runningJobs = 0
            int failedJobs = 0
            int finishedJobs = 0
            int unknownJobs = 0
            for (Job job : arrayChildJobs) {
                if (job.getJobState().isPlannedOrRunning())
                    runningJobs++
                else if (job.getJobState() == JobState.FAILED)
                    failedJobs++
                else if (job.getJobState() == JobState.OK)
                    finishedJobs++
                else if (job.getJobState() == JobState.UNKNOWN)
                    unknownJobs++
            }
            if (failedJobs > 0) return JobState.FAILED
            if (unknownJobs > 0) return JobState.UNKNOWN
            if (runningJobs > 0) return JobState.RUNNING
            return JobState.OK
        }
        return currentJobState != null ? currentJobState : JobState.UNKNOWN
    }

    Command getLastCommand() {
        return lastCommand
    }

    @Override
    String toString() {
        return "Job: ${jobName} calling tool ${tool.getAbsolutePath()}"
    }
}
