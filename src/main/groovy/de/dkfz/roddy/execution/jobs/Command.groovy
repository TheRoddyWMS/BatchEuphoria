/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.StringConstants
import groovy.transform.CompileStatic

import java.util.regex.Matcher

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

    private static final String WORKING_DIRECTORY_DEFAULT = '$HOME'

    /**
     * The id of this command.
     */
    protected final String id
    /**
     * The id which was created upon execution by the job system.
     */
    protected BEJobID executionID

    /**
     * The job which created this command. Can be null!
     */
    protected BEJob creatingJob

    /**
     * Parameters for the qsub command
     */
    public final LinkedHashMap<String, String> parameters = [:]

    /**
     * A list of named tags for the command object
     */
    private final Map<String, Object> commandTags = [:]

    protected final BatchEuphoriaJobManager parentJobManager

    /**
     * A command to be executed on the cluster head node, in particular qsub, bsub, qstat, etc.
     *
     * @param parentJobManager
     * @param job
     * @param id
     * @param parameters       Useful, if the set of parameters used for the execution command is not identical to the Job's parameters.
     * @param commandTags
     */
    protected Command(BatchEuphoriaJobManager parentJobManager, BEJob job, String id, Map<String, String> parameters, Map<String, Object> commandTags) {
        this.parentJobManager = parentJobManager
        this.commandTags.putAll(commandTags ?: [:])
        this.parameters.putAll(parameters ?: [:])
        this.creatingJob = job
        this.id = id
    }

    final void setExecutionID(BEJobID id) {
        this.executionID = id
    }

    final boolean wasExecuted() {
        return executionID.isValidID()
    }

    final BEJobID getExecutionID() {
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

    protected final String getWorkingDirectory(){
        return job.getWorkingDirectory() ?: WORKING_DIRECTORY_DEFAULT
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


    String assembleProcessingCommands() {
        StringBuilder bsubCall = new StringBuilder()
        for (ProcessingParameters pcmd in job.getListOfProcessingParameters()) {
            if (!(pcmd instanceof ProcessingParameters)) continue
            ProcessingParameters command = (ProcessingParameters) pcmd
            if (command == null)
                continue
            bsubCall << StringConstants.WHITESPACE << command.getProcessingCommandString()
        }
        return bsubCall.toString()
    }

    public static final String escapeBash(final String input) {
        "'${input.replaceAll("'", Matcher.quoteReplacement("'\\''"))}'"
    }
}
