/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import groovy.transform.CompileStatic

/**
 * Base class for all types of commands.
 * <p>
 * PBSCommand extends this. Also SGECommand and so on.
 * <p>
 * A job is executed via a command. The command represents the job on the cluster / system side.
 *
 * @author michael
 */
@CompileStatic
abstract class Command {

    protected static final String WORKING_DIRECTORY_DEFAULT = '$HOME'

    /**
     * The job name of this command.
     */
    protected final String jobName
    /**
     * The id which was created upon execution by the job system.
     */
    protected BEJobID jobID

    /**
     * The job which created this command. Can be null!
     */
    protected BEJob creatingJob

    /**
     * Environment variables to be passed with a specific value or as they are declared in the submission environment.
     * null-valued parameters correspond to environment variables to be forwarded as locally defined.
     */
    public final LinkedHashMap<String, String> parameters = [:]

    protected final BatchEuphoriaJobManager parentJobManager

    /**
     * A command to be executed on the cluster head node, in particular qsub, bsub, qstat, etc.
     *
     * @param parentJobManager
     * @param job
     * @param jobName
     * @param environmentVariables
     * @param commandTags
     */
    protected Command(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName, Map<String, String> environmentVariables) {
        this.parentJobManager = parentJobManager
        this.parameters.putAll(environmentVariables ?: [:])
        this.creatingJob = job
        this.jobName = jobName
    }

    final void setJobID(BEJobID id) {
        this.jobID = id
    }

    final boolean wasExecuted() {
        return jobID.isValidID()
    }

    final BEJobID getJobID() {
        return jobID
    }

    final String getID() {
        return this.jobName
    }

    void setJob(BEJob job) {
        this.creatingJob = job
    }

    final BEJob getJob() {
        return creatingJob
    }

    final String getFormattedID() {
        return String.format("command:0x%08X", this.jobName)
    }


    /**
     * Local commands are i.e. blocking, whereas PBSCommands are not.
     * The default is false.
     *
     * @return
     */
    boolean isBlockingCommand() {
        return false
    }

    abstract String toBashCommandString()
}
