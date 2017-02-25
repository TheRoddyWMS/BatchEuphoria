/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs

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
     * Static incremental counter for pipeline commands.
     */
    protected static volatile int idCounter = -1
    /**
     * The id of this command.
     */
    private final String id
    /**
     * The id which was created upon execution by the job system.
     */
    protected JobDependencyID executionID

    /**
     * The job which created this command. Can be null!
     */
    protected Job creatingJob

    /**
     * Parameters for the qsub command
     */
    protected final Map<String, String> parameters = [:]

    /**
     * A list of named tags for the command object
     */
    private final Map<String, Object> commandTags = [:]

    protected final JobManager parentJobManager

    protected Command(JobManager parentJobManager, Job job, String id, Map<String, String> parameters, Map<String, Object> commandTags) {
        this.parentJobManager = parentJobManager
        this.commandTags.putAll(commandTags ?: [:])
        this.parameters.putAll(parameters ?: [:])
        this.creatingJob = job
        this.id = id
    }

    protected static synchronized int getNextIDCountValue() {
        return ++idCounter
    }

    final void setExecutionID(JobDependencyID id) {
        this.executionID = id
    }

    final boolean wasExecuted() {
        return executionID.isValidID()
    }

    final JobDependencyID getExecutionID() {
        return executionID
    }

    final String getID() {
        return id
    }

    void setJob(Job job) {
        this.creatingJob = job
    }

    final Job getJob() {
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


//    public List<ConfigurationValue> getParametersForParameterFile() {
//        List<ConfigurationValue> allParametersForFile = new LinkedList<>();
//        if (parameters.size() > 0) {
//            for (String parm : parameters.keySet()) {
//                String val = parameters.get(parm);
//                if (val.contains(DOLLAR_LEFTBRACE) && val.contains(BRACE_RIGHT)) {
//                    val = val.replace(DOLLAR_LEFTBRACE, "#{"); // Replace variable names so they can be passed to qsub.
//                }
//                String key = parm;
//                allParametersForFile.add(new ConfigurationValue(key, val));
//            }
//        }
//        return allParametersForFile;
//    }

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
