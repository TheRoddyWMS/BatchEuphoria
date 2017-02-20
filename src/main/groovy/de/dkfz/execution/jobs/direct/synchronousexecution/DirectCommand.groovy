/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.execution.jobs.direct.synchronousexecution

import de.dkfz.config.AppConfig
import de.dkfz.eilslabs.tools.constants.StringConstants
import de.dkfz.execution.jobs.Command
import de.dkfz.execution.jobs.Job


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
    private final AppConfig configuration
    private final File loggingDirectory
    public static final String PARM_WRAPPED_SCRIPT = "WRAPPED_SCRIPT="

    public DirectCommand(Job job, AppConfig config, String jobName, List processingCommands, Map<String, String> parameters, List<String> arrayIndices, List<String> dependencyIDs, String command) {
        super(job, jobName,(File) config.getProperty("parameterFile"), parameters);
        //, processingCommands, tool, parameters, dependencies, arraySettings);
        this.processingCommands = processingCommands;
        this.arrayIndices = arrayIndices;
        this.dependencyIDs = dependencyIDs;
        this.command = command;
        this.configuration = config
        this.loggingDirectory = new File(configuration.getProperty("LoggingDirectoryPath",""))
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
                if (val.contains(StringConstants.DOLLAR_LEFTBRACE) && val.contains(StringConstants.BRACE_RIGHT)) {
                    val = val.replace(StringConstants.DOLLAR_LEFTBRACE, "#{"); // Replace variable names so they can be passed to qsub.
                }
                parameterBuilder << " ${pName}=${val}";
        }

        parameterBuilder << StringConstants.WHITESPACE << PARM_WRAPPED_SCRIPT << command;

        //TODO Log handling

        //TODO Array handling

        //TODO email handling? Better not

        //Dependencies are ignored

        //Grouplist is ignored

        //Umask is ignored

        //Processing commands are ignored

        //TODO Command assembly should be part of the file system provider? Maybe there is a need for a local file system provider?
        //This is very linux specific...
        commandString << parameterBuilder.toString() << StringConstants.WHITESPACE << configuration.getProperty("ProcessingToolPath","/");

        return commandString.toString();
    }


}
