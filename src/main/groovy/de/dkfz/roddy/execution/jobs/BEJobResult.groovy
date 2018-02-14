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
    final Command command
    /**
     * The current job's id, i.e. qsub id.
     * Used for dependencies.
     */
    final BEJob job
    /**
     * The execution result object containing additional details about the execution (exit code and output).
     */
    final ExecutionResult executionResult
    /**
     * The tool which was run for this job.
     */
    final File toolID;
    /**
     * Parameters for the job.
     */
    final Map<String, String> jobParameters;
    /**
     * Parent jobs.
     */
    public transient final List<BEJob> parentJobs;

    // Compatibility constructor. Does nothing, leaves responsibility in sub class.
    protected BEJobResult() {

    }

    BEJobResult(Command command, BEJob job, ExecutionResult executionResult, File toolID, Map<String, String> jobParameters, List<BEJob> parentJobs) {
        this.command = command;
        assert (null != job)
        this.job = job;
        this.executionResult = executionResult
        this.toolID = toolID;
        this.jobParameters = jobParameters;
        this.parentJobs = parentJobs;
    }

    BEJobID getJobID() {
        return job.jobID
    }

    boolean isWasExecuted() {
        return null != executionResult && executionResult.successful
    }

}
