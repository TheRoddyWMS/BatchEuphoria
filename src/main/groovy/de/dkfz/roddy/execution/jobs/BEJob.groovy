/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import com.google.common.base.Preconditions
import de.dkfz.roddy.config.EmptyResourceSet
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.tools.EscapableString
import de.dkfz.roddy.tools.BashInterpreter
import de.dkfz.roddy.execution.Code
import de.dkfz.roddy.execution.CommandI
import de.dkfz.roddy.execution.CommandReferenceI
import groovy.transform.CompileStatic
import org.jetbrains.annotations.NotNull
import org.jetbrains.annotations.Nullable

import java.util.concurrent.atomic.AtomicLong

import static de.dkfz.roddy.tools.EscapableString.Shortcuts.*

/**
 * The job class represents a generic and abstract form of cluster job which can be run using a job manager.
 * When a job is executed with the JM, the used Command and a BEJobResult object will be created and added to this
 * object.
 */
@CompileStatic
class BEJob<J extends BEJob, JR extends BEJobResult> implements Comparable<BEJob> {

    public static final String PARM_JOBCREATIONCOUNTER = "JOB_CREATION_COUNTER"

    private static AtomicLong absoluteJobCreationCounter = new AtomicLong()

    /**
     * The identifier of the job as returned by the batch processing system.
     */
    private BEJobID jobID

    /**
     * The descriptive name of the job. Can be passed to the execution system.
     */
    private final EscapableString jobNameEscapable

    /**
     * The accounting name under which the job runs. It is the responsibility of the execution system to use this
     * information.
     */
    private final EscapableString accountingName


    /**
     * Jobs can be marked as dirty if they are in a directed acyclic graph of job dependency modelling a workflow.
     */
    protected boolean isDirty

    /**
     * An internal job creation count. Has nothing to do with e.g. PBS / cluster / process id's!
     */
    protected final long jobCreationCounter = absoluteJobCreationCounter.incrementAndGet()

    /**
     * The command to be executed as BEJob on the cluster.
     */
    protected CommandI commandObj

    /**
     * The set of resources for this tool / BEJob
     * Contains values for e.g. maxMemory, walltime and so on.
     */
    protected ResourceSet resourceSet

    /**
     * Parameters for the tool you want to call
     */
    protected final Map<String, EscapableString> parameters

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

    @Nullable
    protected File workingDirectory

    /**
     * Set this to use a custom queue for the job. (If supported by the target job manager)
     */
    String customQueue

    private JobLog jobLog

    BatchEuphoriaJobManager jobManager

    BEJob(BEJobID jobID,                        // can be null for FakeBEJob
          BatchEuphoriaJobManager jobManager,   // can be null for FakeBEJob
          EscapableString jobNameEscapable = null,
          CommandI commandObj = null,
          @NotNull ResourceSet resourceSet = new EmptyResourceSet(),
          @NotNull Collection<BEJob> parentJobs = [],
          @NotNull Map<String, EscapableString> parameters = [:],
          @NotNull JobLog jobLog = JobLog.none(),
          File workingDirectory = null,
          EscapableString accountingName = null) {
        this.jobID = Optional.ofNullable(jobID).orElse(BEJobID.getNewUnknown())
        this.jobNameEscapable = jobNameEscapable
        this.currentJobState = JobState.UNSTARTED
        this.commandObj = commandObj
        Preconditions.checkArgument(resourceSet != null)
        this.resourceSet = resourceSet
        Preconditions.checkArgument(parameters != null)
        this.parameters = parameters
        this.jobManager = jobManager
        Preconditions.checkArgument(jobLog != null)
        this.jobLog = jobLog
        this.workingDirectory = workingDirectory
        this.accountingName = accountingName
        Preconditions.checkArgument(parentJobs != null)
        this.addParentJobs(parentJobs)
    }

    BEJob addParentJobs(@NotNull Collection<BEJob> parentJobs) {
        Preconditions.checkArgument(parentJobs != null)
        this.parentJobs.addAll(parentJobs)
        return this
    }

    BEJob addParentJobIDs(@NotNull List<BEJobID> parentJobIDs,
                          @NotNull BatchEuphoriaJobManager jobManager) {
        Preconditions.checkArgument(parentJobIDs != null)
        this.parentJobs.addAll(parentJobIDs.collect { new BEJob(it, jobManager) })
        return this
    }

    JR getRunResult() {
        return this.runResult
    }

    BEJob setRunResult(JR result) {
        Preconditions.checkArgument(this.jobID == result.jobID)
        this.runResult = result
        return this
    }

    boolean isFakeJob() {
        jobID.toString()
        if (this instanceof FakeBEJob)
            return true
        if (jobNameEscapable != null && jobNameEscapable.equals("Fakejob"))
            return true
        String jobID = jobID
        if (jobID == null)
            return false
        return BEFakeJobID.isFakeJobID(jobID)
    }

    void addProcessingParameters(ProcessingParameters processingParameters) {
        if (processingParameters == null) return
        this.processingParameters.add(processingParameters)
    }

    Map<String, EscapableString> getParameters() {
        return parameters
    }

    List<ProcessingParameters> getListOfProcessingParameters() {
        return [jobManager.convertResourceSet(this)] + processingParameters
    }

    List<J> getParentJobs() {
        return parentJobs as List<J>
    }

    static List<BEJob> jobsWithUniqueValidJobId(List<BEJob> jobs) {
        return jobs.
                findAll { !it.fakeJob }.
                sort { it.jobID.toString() }.
                unique { it.jobID.toString() }
    }

    static List<BEJobID> uniqueValidJobIDs(List<BEJobID> jobIDs) {
        return jobIDs.
                findAll { it.validID }.
                sort { it.toString() }.
                unique { it.toString() }
    }

    List<BEJobID> getParentJobIDs() {
        return parentJobs.collect { it.jobID }
    }

    @Nullable File getWorkingDirectory() {
        return this.workingDirectory
    }

    void resetJobID(@NotNull BEJobID jobID) {
        Preconditions.checkArgument(jobID != null)
        this.jobID = jobID
    }

    void resetJobID() {
        resetJobID(BEJobID.getNewUnknown())
    }


    BEJobID getJobID() {
        return this.jobID
    }

    EscapableString getAccountingName() {
        return this.accountingName
    }

    boolean getIsDirty() {
        return this.isDirty
    }

    long getJobCreationCounter() {
        return this.jobCreationCounter
    }

    @Nullable CommandI getCommandObj() {
        return commandObj
    }

    ResourceSet getResourceSet() {
        return resourceSet
    }

    String getJobName() {
        return forBash(jobNameEscapable)
    }

    EscapableString getJobNameEscapable() {
        return jobNameEscapable
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
        if (this.code) {
            return "BEJob: ${jobName} with code:\n\t" +
                    BashInterpreter.instance.interpret(code).
                            split("\n").
                            join("\n\t")
        } else {
            return "BEJob: ${jobName} calling command " +
                    BashInterpreter.instance.interpret(join(command, " "))
        }
    }

    @Override
    int compareTo(BEJob o) {
        return this.jobID.compareTo(o.jobID)
    }

    /** The following methods simplify the porting of the old code to the CommandI-based handling
     *  of tools, tool scripts, and (new) commands (with arguments).
     */
    File getExecutableFile() {
        if (commandObj instanceof CommandReferenceI) {
            (commandObj as CommandReferenceI).executablePath.toFile()
        } else {
            null
        }
    }

    List<EscapableString> getCommand() {
        if (commandObj instanceof CommandReferenceI) {
            (commandObj as CommandReferenceI).toCommandSegmentList()
        } else {
            null
        }
    }

    EscapableString getCode() {
        if (commandObj instanceof Code) {
            (commandObj as Code).toEscapableString()
        } else {
            null
        }
    }
}
