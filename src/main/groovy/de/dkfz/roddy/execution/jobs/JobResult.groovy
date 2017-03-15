/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.eilslabs.batcheuphoria.jobs.Command
import de.dkfz.eilslabs.batcheuphoria.jobs.Job
import de.dkfz.roddy.core.InfoObject
import groovy.transform.CompileStatic

/**
 * Compatibility interface for Roddy and Roddy plugins.
 * Without it, all plugins depending on 2.3 would need to be adapted and recompiled!
 * Created by heinold on 01.03.17.
 */
@Deprecated
@CompileStatic
class JobResult {
/**
 * The command which was used to create this result.
 */
    protected final Command command;
    /**
     * The current job's id, i.e. qsub id.
     * Used for dependencies.
     */
    protected final JobDependencyID jobID;
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
    public transient final List<Job> parentJobs;

    // Compatibility constructor. Does nothing, leaves responsibility in sub class.
    protected JobResult() {

    }

    public JobResult(InfoObject object, Command command, JobDependencyID jobID, boolean wasExecuted, File toolID, Map<String, String> jobParameters, List<Job> parentJobs) {
        this(command, jobID, wasExecuted, false, toolID, jobParameters, parentJobs)
    }

    public JobResult(Command command, JobDependencyID jobID, boolean wasExecuted, File toolID, Map<String, String> jobParameters, List<Job> parentJobs) {
        this(command, jobID, wasExecuted, false, toolID, jobParameters, parentJobs)
    }

    public JobResult(Command command, JobDependencyID jobID, boolean wasExecuted, boolean wasArray, File toolID, Map<String, String> jobParameters, List<Job> parentJobs) {
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

    public JobDependencyID getJobID() {
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

    public Job getJob() {
        return jobID.job;
    }

    public Map<String, String> getJobParameters() {
        return jobParameters;
    }

    public List<Job> getParentJobs() {
        return parentJobs;
    }
}