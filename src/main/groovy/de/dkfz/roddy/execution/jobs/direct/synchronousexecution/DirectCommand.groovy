/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.direct.synchronousexecution

import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import groovy.transform.CompileStatic

/**
 * Local commands run locally and, if the workflow requires and supports it, concurrent.
 * They are called in a local process with waitFor after each call. Dependencies are therefore automatically resolved.
 * Roddy waits for the processes to exit.
 */
@CompileStatic
class DirectCommand extends Command {

    private final List<ProcessingParameters> processingParameters
    public static final String PARM_WRAPPED_SCRIPT = "WRAPPED_SCRIPT="


    DirectCommand(DirectSynchronousExecutionJobManager parentManager,
                  BEJob job,
                  List<ProcessingParameters> processingParameters) {
        super(parentManager, job, job.executableFile.name, job.parameters)
        this.processingParameters = processingParameters
    }

    /**
     * Local commands are always blocking.
     * @return
     */
    @Override
    boolean isBlockingCommand() {
        return true
    }

    @Override
    String toString() {
        return toBashCommandString()
    }

    @Override
    String toBashCommandString() {

        StringBuilder commandString = new StringBuilder()

        StringBuilder parameterBuilder = new StringBuilder()

        parameterBuilder << parameters.collect { key, value -> "${key}=${value}" }.join(" ")

        // Grouplist is ignored
        // Umask is ignored

        // Command assembly should be part of the file system provider?
        // Maybe there is a need for a local file system provider?

        // This is very linux specific...
        commandString <<
                parameterBuilder.toString() <<
                StringConstants.WHITESPACE <<
                job.getCommand(true).join(StringConstants.WHITESPACE) <<
                " &> ${job.jobLog.getOut(job.jobCreationCounter.toString())}";

        return commandString.toString()
    }
}
