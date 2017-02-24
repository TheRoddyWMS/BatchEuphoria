/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.List;

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
    private final Command command;
    /**
     * The current job's id, i.e. qsub id.
     * Used for dependencies.
     */
    private final JobDependencyID jobID;
    /**
     * Was the job executed?
     */
    private final boolean wasExecuted;
    /**
     * Was the job an array job?
     */
    private final boolean wasArray;
    /**
     * The tool which was run for this job.
     */
    private final File toolID;
    /**
     * Parameters for the job.
     */
    private final Map<String, String> jobParameters;
    /**
     * Parent jobs.
     */
    public transient final List<Job> parentJobs;

    public JobResult(Command command, JobDependencyID jobID, boolean wasExecuted, File toolID, Map<String, String> jobParameters, List<Job> parentJobs) {
        this(command, jobID, wasExecuted, false, toolID, jobParameters, parentJobs);
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


    private synchronized void writeObject(java.io.ObjectOutputStream s) throws IOException {
        try {
            s.defaultWriteObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
        try {
            s.defaultReadObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
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
