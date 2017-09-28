/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.execution.io.ExecutionResult
import groovy.transform.CompileStatic

/**
 * Result of a job run.
 * <p/>
 * Stores different information related to a job run. i.e. if the job was
 * executed.
 *
 * @author michael
 */
@CompileStatic
class BEJobResult implements Serializable {

    /**
     * The command which was used to create this result.
     */
    protected final Command command
    /**
     * The current job's id, i.e. qsub id.
     * Used for dependencies.
     */
    protected final BEJob job
    /**
     * The execution result object containing additional details about the execution (exit code and output).
     */
    protected final ExecutionResult executionResult
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
    protected BEJobResult() {

    }

    BEJobResult(Command command, BEJob job, ExecutionResult res, File toolID, Map<String, String> jobParameters, List<BEJob> parentJobs) {
        this(command, job, res, false, toolID, jobParameters, parentJobs)
    }

    BEJobResult(Command command, BEJob job, ExecutionResult executionResult, boolean wasArray, File toolID, Map<String, String> jobParameters, List<BEJob> parentJobs) {
        this.command = command;
        assert (null != job)
        this.job = job;
        this.executionResult = executionResult
        this.wasArray = wasArray;
        this.toolID = toolID;
        this.jobParameters = jobParameters;
        this.parentJobs = parentJobs;
    }

    public Command getCommand() {
        return command;
    }

    public BEJobID getJobID() {
        return job.jobID
    }

    public boolean isWasExecuted() {
        return null != executionResult && executionResult.successful
    }

    public boolean isWasArray() {
        return wasArray;
    }

    public File getToolID() {
        return toolID;
    }

    public BEJob getJob() {
        return job
    }

    public Map<String, String> getJobParameters() {
        return jobParameters;
    }

    public List<BEJob> getParentJobs() {
        return parentJobs;
    }

}
