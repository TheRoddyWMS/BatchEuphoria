/*
 * Copyright (c) 2023 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import com.google.common.base.Preconditions
import de.dkfz.roddy.execution.io.ExecutionResult
import groovy.transform.CompileStatic
import org.jetbrains.annotations.NotNull

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
    final BECommand beCommand
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
     * Parameters for the job.
     */
    final Map<String, String> jobParameters
    /**
     * Parent jobs.
     */
    public transient final List<BEJob> parentJobs

    // Compatibility constructor. Does nothing, leaves responsibility in sub class.
    protected BEJobResult() {

    }

    /** The tool parameter should be removed. This is only kept for backwards compatibility. */
    @Deprecated
    BEJobResult(BECommand beCommand, BEJob job, ExecutionResult executionResult, File tool,
                Map<String, String> jobParameters, List<BEJob> parentJobs) {
        this.beCommand = beCommand
        Preconditions.checkArgument(job != null)
        Preconditions.checkArgument(tool == job.executableFile)
        this.job = job
        this.executionResult = executionResult
        this.jobParameters = jobParameters
        this.parentJobs = parentJobs
        // NOTE: tool is not used anymore.
    }
    
    BEJobResult(BECommand beCommand,
                @NotNull BEJob job,
                ExecutionResult executionResult,
                Map<String, String> jobParameters,
                List<BEJob> parentJobs) {
        this.beCommand = beCommand
        Preconditions.checkArgument(job != null)
        this.job = job
        this.executionResult = executionResult
        this.jobParameters = jobParameters
        this.parentJobs = parentJobs
    }

    BEJobID getJobID() {
        return job.jobID
    }

    Optional<List<String>> getResultLines() {
        return Optional.of(executionResult).map { it.stdout } as Optional<List<String>>
    }

    boolean isSuccessful() {
        return null != executionResult && executionResult.successful
    }

}
