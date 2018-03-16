/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet

import java.util.concurrent.atomic.AtomicLong

/**
 * The job class represents a generic and abstract form of cluster job which can be run using a job manager.
 * When a job is executed with the JM, the used Command and a BEJobResult object will be created and added to this
 * object.
 */
@groovy.transform.CompileStatic
class BEJob<J extends BEJob, JR extends BEJobResult> implements Comparable<BEJob> {

    private static final de.dkfz.roddy.tools.LoggerWrapper logger = de.dkfz.roddy.tools.LoggerWrapper.getLogger(BEJob.class.getSimpleName())

    public static final String PARM_JOBCREATIONCOUNTER = "JOB_CREATION_COUNTER"

    private static AtomicLong absoluteJobCreationCounter = new AtomicLong()

    /**
     * The identifier of the job as returned by the batch processing system.
     */
    private BEJobID jobID

    /**
     * The destriptive name of the job. Can be passed to the execution system.
     */
    public final String jobName

    /**
     * Jobs can be marked as dirty if they are in a directed acyclic graph of job dependency modelling a workflow.
     */
    public boolean isDirty

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
     * A tool script (the actual code!) which will be piped (or whatever...) to the job manager submission command / method
     * It is either testScript OR tool
     */
    String toolScript
    /**
     * The set of resources for this tool / BEJob
     * Contains values for e.g. maxMemory, walltime and so on.
     */
    protected ResourceSet resourceSet

    /**
     * Parameters for the tool you want to call
     */
    protected final Map<String, String> parameters

    protected SortedSet<BEJob> parentJobs = new TreeSet<BEJob>()

    /**
     * Temporary value which defines the jobs jobState.
     */
    protected transient JobState currentJobState

    private List<ProcessingParameters> processingParameters = new LinkedList<ProcessingParameters>()

    /**
     * Stores the result when the job was executed.
     */
    private JR runResult

    //////////////////////////////////////////////////////////////
    // Now come some job / command specific settings
    //////////////////////////////////////////////////////////////

    protected File workingDirectory

    /**
     * Set this, to have use a custom account for the job. (If supported by the target job manager)
     */
    String customUserAccount

    /**
     * Set this to use a custom queue for the job. (If supported by the target job manager)
     */
    String customQueue

    private JobLog jobLog

    BatchEuphoriaJobManager jobManager

    BEJob(BEJobID jobID, String jobName, File tool, String toolScript, String toolMD5, ResourceSet resourceSet, Collection<BEJob> parentJobs,
          Map<String, String> parameters, BatchEuphoriaJobManager jobManager, JobLog jobLog, File workingDirectory) {
        this.jobID = Optional.ofNullable(jobID).orElse(new BEJobID())
        this.jobName = jobName
        this.currentJobState = JobState.UNSTARTED
        this.tool = tool
        this.toolScript = toolScript
        // FakeJobs have neither tool nor toolScript set.
        if (tool && toolScript)
            throw new BEException("A job must have exactly one of tool and toolScript set. Found: tool=${tool}, toolScript=${toolScript}")
        this.toolMD5 = toolMD5
        this.resourceSet = resourceSet
        this.parameters = parameters
        this.jobManager = jobManager
        assert jobLog : "jobLog not set"
        this.jobLog = jobLog
        this.workingDirectory = workingDirectory
        this.addParentJobs(Optional.ofNullable(parentJobs).orElse([]))
    }

    BEJob(BEJobID jobID, BatchEuphoriaJobManager jobManager) {
        this(jobID, null, null, null, null, null, [], [:], jobManager, JobLog.none(), null)
    }

    BEJob addParentJobs(Collection<BEJob> parentJobs) {
        assert (null != parentJobs)
        this.parentJobs.addAll(parentJobs)
        return this
    }

    BEJob addParentJobIDs(List<BEJobID> parentJobIDs, BatchEuphoriaJobManager jobManager) {
        assert (null != parentJobIDs)
        this.parentJobs.addAll(parentJobIDs.collect { new BEJob(it, jobManager) })
        return this
    }

    BEJobResult getRunResult() {
        return this.runResult
    }

    BEJob setRunResult(BEJobResult result) {
        assert (this.jobID == result.jobID)
        this.runResult = result
        return this
    }

    boolean isFakeJob() {
        getJobID().toString()
        if (this instanceof FakeBEJob)
            return true
        if (jobName != null && jobName.equals("Fakejob"))
            return true
        String jobID = getJobID()
        if (jobID == null)
            return false
        return BEFakeJobID.isFakeJobID(jobID)
    }

    void addProcessingParameters(ProcessingParameters processingParameters) {
        if (processingParameters == null) return
        this.processingParameters.add(processingParameters)
    }

    Map<String, String> getParameters() {
        return parameters
    }

    List<ProcessingParameters> getListOfProcessingParameters() {
        return [jobManager.convertResourceSet(this)] + processingParameters
    }

    List<J> getParentJobs() {
        return parentJobs as List<J>
    }

    static List<BEJob> jobsWithUniqueValidJobId(List<BEJob> jobs) {
        return jobs.findAll { !it.isFakeJob() }.sort { it.getJobID().toString() }.unique { it.getJobID().toString() }
    }

    static List<BEJobID> uniqueValidJobIDs(List<BEJobID> jobIDs) {
        return jobIDs.findAll { it.isValidID() }.sort { it.toString() }.unique { it.toString() }
    }

    List<BEJobID> getParentJobIDs() {
        return parentJobs.collect { it.getJobID() }
    }

    File getWorkingDirectory() {
        return this.workingDirectory
    }

    void resetJobID(BEJobID jobID) {
        assert (null != jobID)
        this.jobID = jobID
    }

    void resetJobID() {
        resetJobID(new BEJobID())
    }


    BEJobID getJobID() {
        return this.jobID
    }

    File getTool() {
        return tool
    }

    String getToolScript() {
        return toolScript
    }

    String getToolMD5() {
        return toolMD5
    }

    ResourceSet getResourceSet() {
        return resourceSet
    }

    String getJobName() {
        return jobName
    }

    void setJobState(JobState js) {
        this.currentJobState = js
    }

    JobState getJobState() {
        return currentJobState != null ? currentJobState : JobState.UNKNOWN
    }

    JobLog getJobLog() {
        return jobLog
    }

    @Override
    String toString() {
        if (getToolScript()) {
            return "BEJob: ${jobName} with piped script:\n\t" + getToolScript().readLines().join("\n\t")
        } else {
            return "BEJob: ${jobName} calling tool ${tool?.getAbsolutePath()}"
        }
    }

    @Override
    int compareTo(BEJob o) {
        return this.jobID.compareTo(o.jobID)
    }
}
