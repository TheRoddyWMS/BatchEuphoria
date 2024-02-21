/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ)..
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster

import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.execution.CommandI
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.SubmissionCommand
import de.dkfz.roddy.tools.BashUtils
import de.dkfz.roddy.tools.shell.bash.Service
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.EMPTY


@CompileStatic
abstract class GridEngineBasedSubmissionCommand extends SubmissionCommand {

    public static final String NONE = "none"

    /**
     * A command to submit jobs from the cluster head node, in particular qsub, bsub, qstat, etc.
     *
     * @param parentJobManager
     * @param job
     * @param jobName
     * @param processingParameter s@param environmentVariables *@param dependencyIDs @param command
     */
    GridEngineBasedSubmissionCommand(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName,
                                     List<ProcessingParameters> processingParameters,
                                     Map<String, String> environmentVariables,
                                     List<String> dependencyIDs) {
        super(parentJobManager, job, jobName, processingParameters, environmentVariables, dependencyIDs)
    }


    @Override
    protected String assembleDependencyParameter(List<BEJobID> jobIds) {
        StringBuilder qsubCall = new StringBuilder("")
        LinkedList<String> tempDependencies =
                jobIds.findAll {
                    it.getId() != "" && it.getId() != NONE && it.getId() != "-1"
                }.collect {
                    it.getId().split("\\.")[0] // Keep the command line short. GE accepts the job number for dependencies.
                } as LinkedList<String>
        if (tempDependencies.size() > 0) {
            qsubCall <<
                    getDependsSuperParameter() <<
                    getDependencyParameterName() <<
                    getDependencyOptionSeparator() <<
                    tempDependencies.join(getDependencyIDSeparator())
        }

        return qsubCall
    }

    protected abstract String getDependsSuperParameter()

    protected abstract String getDependencyParameterName()

    protected abstract String getDependencyOptionSeparator()

    protected abstract String getDependencyIDSeparator()

    @Override
    protected Boolean getQuoteCommand() {
        true
    }

    @Override
    String getSubmissionExecutableName() {
        return "qsub"
    }

    @Override
    protected String composeCommandString(List<String> parameters) {
                StringBuilder command = new StringBuilder(EMPTY)

        if (job.code) {
            command <<
                "echo -ne " <<
                escapeScriptForEval(job.code) <<
                " | "
        }

        command << getSubmissionExecutableName()

        command << " ${parameters.join(" ")} "

        if (job.command) {
            // Commands that are appended to the submission command and its parameters, e.g.,
            // in `bsub ... command ...` need to be quoted to prevent that expressions and
            // variables are evaluated on the submission site instead of the actual remote
            // cluster node.
            // This won't have any effect unless you have Bash special characters in your command.
            List<String> commandToBeExecuted = job.command
            if (quoteCommand) {
                commandToBeExecuted = commandToBeExecuted.collect { segment ->
                    Service.escape(segment)
                }
            }
            command << " " << commandToBeExecuted.join(StringConstants.WHITESPACE)
        }

        return command
    }
}
