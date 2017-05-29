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

    /**
     * The id of this command.
     */
    protected final String id
    /**
     * The id which was created upon execution by the job system.
     */
    protected de.dkfz.roddy.execution.jobs.JobDependencyID executionID

    /**
     * The job which created this command. Can be null!
     */
    protected BEJob creatingJob

    /**
     * Parameters for the qsub command
     */
    protected final Map<String, String> parameters = [:]

    /**
     * A list of named tags for the command object
     */
    private final Map<String, Object> commandTags = [:]

    protected final BatchEuphoriaJobManager parentJobManager

    protected Command(BatchEuphoriaJobManager parentJobManager, BEJob job, String id, Map<String, String> parameters, Map<String, Object> commandTags) {
        this.parentJobManager = parentJobManager
        this.commandTags.putAll(commandTags ?: [:])
        this.parameters.putAll(parameters ?: [:])
        this.creatingJob = job
        this.id = id
    }

    final void setExecutionID(de.dkfz.roddy.execution.jobs.JobDependencyID id) {
        this.executionID = id
    }

    final boolean wasExecuted() {
        return executionID.isValidID()
    }

    final de.dkfz.roddy.execution.jobs.JobDependencyID getExecutionID() {
        return executionID
    }

    final String getID() {
        return id
    }

    void setJob(BEJob job) {
        this.creatingJob = job
    }

    final BEJob getJob() {
        return creatingJob
    }

    final String getFormattedID() {
        return String.format("command:0x%08X", id)
    }

    boolean hasTag(String tagID) {
        return commandTags.containsKey(tagID)
    }

    Object getTag(String tagID) {
        return commandTags[tagID]
    }

    Map<String, Object> getTags() {
        return commandTags
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

    @Override
    String toString() {
        return String.format("Command of class %s with id %s", this.getClass().getName(), getID())
    }
}
