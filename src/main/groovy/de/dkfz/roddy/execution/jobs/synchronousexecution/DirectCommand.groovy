/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.synchronousexecution

import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.ProcessingCommands
import de.dkfz.roddy.StringConstants

import static de.dkfz.roddy.StringConstants.BRACE_RIGHT
import static de.dkfz.roddy.StringConstants.DOLLAR_LEFTBRACE

/**
 * Local commands run locally and, if the workflow requires and supports it, concurrent.
 * They are called in a local process with waitFor after each call. Dependencies are therefore automatically resolved.
 * Roddy waits for the processes to exit.
 */
@groovy.transform.CompileStatic
public class DirectCommand extends Command {

    private final List processingCommands;
    private final List<String> arrayIndices;
    private final List<String> dependencyIDs;
    private final String command;
    private final File loggingDirectory
    public static final String PARM_WRAPPED_SCRIPT = "WRAPPED_SCRIPT="


    public DirectCommand(DirectSynchronousExecutionJobManager parentManager, BEJob job, String id, List<ProcessingCommands> processingCommands, Map<String, String> parameters, Map<String, Object> tags, List<String> arrayIndices, List<String> dependencyIDs, String command, File loggingDirectory) {
        super(parentManager, job, id, parameters, tags)
        //, processingCommands, tool, parameters, dependencies, arraySettings);
        this.processingCommands = processingCommands;
        this.arrayIndices = arrayIndices;
        this.dependencyIDs = dependencyIDs;
        this.command = command;
        this.loggingDirectory = loggingDirectory
    }

    /**
     * Local commands are always blocking.
     * @return
     */
    @Override
    public boolean isBlockingCommand() {
        return true;
    }

    @Override
    public String toString() {

        StringBuilder commandString = new StringBuilder();

        StringBuilder parameterBuilder = new StringBuilder();
        parameters.each {
            String pName, String val ->
                //TODO Code dedup with PBSCommand
                if (val.contains(DOLLAR_LEFTBRACE) && val.contains(BRACE_RIGHT)) {
                    val = val.replace(DOLLAR_LEFTBRACE, "#{"); // Replace variable names so they can be passed to qsub.
                }
                parameterBuilder << " ${pName}=${val}";
        }

//        parameterBuilder << StringConstants.WHITESPACE << PARM_WRAPPED_SCRIPT << command;

        //TODO Log handling

        //TODO Array handling

        //TODO email handling? Better not

        //Dependencies are ignored

        //Grouplist is ignored

        //Umask is ignored

        //Processing commands are ignored

        //TODO Command assembly should be part of the file system provider? Maybe there is a need for a local file system provider?
        //This is very linux specific...
        commandString << parameterBuilder.toString() << StringConstants.WHITESPACE << command; // << job.configuration.getProcessingToolPath(executionContext, "wrapinScript").getAbsolutePath();

        return commandString.toString();
    }
}