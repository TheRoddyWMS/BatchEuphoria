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

        // Taken from PBSCommand. Really needs to be unified!
        if (job.parameters.containsKey("CONFIG_FILE") && job.parameters.containsKey("PARAMETER_FILE")) {
            parameterBuilder << "CONFIG_FILE=" << job.parameters["CONFIG_FILE"] << " PARAMETER_FILE=" << job.parameters["PARAMETER_FILE"]
        } else {
            parameterBuilder << parameters.collect { key, value -> "${key}=${value}" }.join(" ")
        }

        //TODO Log handling

        //TODO email handling? Better not

        //Dependencies are ignored

        //Grouplist is ignored

        //Umask is ignored

        //Processing commands are ignored

        //TODO Command assembly should be part of the file system provider? Maybe there is a need for a local file system provider?
        //This is very linux specific...
        commandString << parameterBuilder.toString() << StringConstants.WHITESPACE << command; // << job.configuration.getProcessingToolPath(executionContext, "wrapinScript").getAbsolutePath();

        return commandString.toString()
    }
}
