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

import static de.dkfz.roddy.StringConstants.BRACE_RIGHT
import static de.dkfz.roddy.StringConstants.DOLLAR_LEFTBRACE

/**
 * Local commands run locally and, if the workflow requires and supports it, concurrent.
 * They are called in a local process with waitFor after each call. Dependencies are therefore automatically resolved.
 * Roddy waits for the processes to exit.
 */
@groovy.transform.CompileStatic
class DirectCommand extends Command {

    private final List<ProcessingParameters> processingParameters
    private final String command
    public static final String PARM_WRAPPED_SCRIPT = "WRAPPED_SCRIPT="


    DirectCommand(DirectSynchronousExecutionJobManager parentManager, BEJob job, List<ProcessingParameters> processingParameters, @Deprecated String command = null) {
        super(parentManager, job, job.tool.getName(), job.parameters)
        this.processingParameters = processingParameters
        this.command = command ?: job.tool.absolutePath
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

        // Dependencies are ignored. Direct commands are executed in-sync.

        // Processing commands are ignored BE does not offer job scheduling on its own.

        //TODO email handling? Better not

        //TODO Grouplist is ignored

        //TODO Umask is ignored

        //TODO Command assembly should be part of the file system provider? Maybe there is a need for a local file system provider?
        //This is very linux specific...
        commandString << parameterBuilder.toString() << StringConstants.WHITESPACE << command << " &> ${job.jobLog.getOut(job.jobCreationCounter.toString())} & wait &> /dev/null";

        return commandString.toString()
    }
}
