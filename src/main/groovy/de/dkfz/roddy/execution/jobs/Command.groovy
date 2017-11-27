/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.JobLog
import groovy.transform.CompileStatic

import java.util.regex.Matcher

import static de.dkfz.roddy.StringConstants.EMPTY

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
     * Parameters for the qsub command
     */
    public final LinkedHashMap<String, String> parameters = [:]

    protected final BatchEuphoriaJobManager parentJobManager

    /**
     * A command to be executed on the cluster head node, in particular qsub, bsub, qstat, etc.
     *
     * @param parentJobManager
     * @param job
     * @param jobName
     * @param parameters       Useful, if the set of parameters used for the execution command is not identical to the Job's parameters.
     * @param commandTags
     */
    protected Command(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName, Map<String, String> parameters) {
        this.parentJobManager = parentJobManager
        this.parameters.putAll(parameters ?: [:])
        this.creatingJob = job
        this.jobName = jobName
    }

    final void setJobID(BEJobID id) {
        this.jobID = id
    }

    final boolean wasExecuted() {
        return jobID.isValidID()
    }

    final BEJobID setJobID() {
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

    public static final String escapeBash(final String input) {
        "'${input.replaceAll("'", Matcher.quoteReplacement("'\\''"))}'"
    }
}
