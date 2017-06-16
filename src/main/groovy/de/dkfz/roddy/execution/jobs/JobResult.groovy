/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs;

import de.dkfz.roddy.core.InfoObject

/**
 * Result of a job run.
 * <p/>
 * Stores different information related to a job run. i.e. if the job was
 * executed.
 *
 * @author michael
 */
public class JobResult implements Serializable {

    /**
     * The command which was used to create this result.
     */
    protected final Command command;
    /**
     * The current job's id, i.e. qsub id.
     * Used for dependencies.
     */
    protected final BEJobDependencyID jobID;
    /**
     * Was the job executed?
     */
    protected final boolean wasExecuted;
    /**
     * Was the job an array job?
     */
    protected final boolean wasArray;
    /**
     * The tool which was run for this job.
     */
    protected final File toolID;
    /**
     * Parameters for the job.
     */
    protected final Map<String, String> jobParameters;
    /**
     * Parent jobs.
     */
    public transient final List<BEJob> parentJobs;

    // Compatibility constructor. Does nothing, leaves responsibility in sub class.
    protected JobResult() {

    }

    public JobResult(InfoObject object, Command command, BEJobDependencyID jobID, boolean wasExecuted, File toolID, Map<String, String> jobParameters, List<BEJob> parentJobs) {
        this(command, jobID, wasExecuted, false, toolID, jobParameters, parentJobs)
    }

    public JobResult(Command command, BEJobDependencyID jobID, boolean wasExecuted, File toolID, Map<String, String> jobParameters, List<BEJob> parentJobs) {
        this(command, jobID, wasExecuted, false, toolID, jobParameters, parentJobs)
    }

    public JobResult(Command command, BEJobDependencyID jobID, boolean wasExecuted, boolean wasArray, File toolID, Map<String, String> jobParameters, List<BEJob> parentJobs) {
        this.command = command;
        this.jobID = jobID;
        this.wasExecuted = wasExecuted;
        this.wasArray = wasArray;
        this.toolID = toolID;
        this.jobParameters = jobParameters;
        this.parentJobs = parentJobs;
    }

    public Command getCommand() {
        return command;
    }

    public BEJobDependencyID getJobID() {
        return jobID;
    }

    public boolean isWasExecuted() {
        return wasExecuted;
    }

    public boolean isWasArray() {
        return wasArray;
    }

    public File getToolID() {
        return toolID;
    }

    public BEJob getJob() {
        return jobID.job;
    }

    public Map<String, String> getJobParameters() {
        return jobParameters;
    }

    public List<BEJob> getParentJobs() {
        return parentJobs;
    }

}
