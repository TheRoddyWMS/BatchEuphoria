/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ)..
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster

import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.execution.AnyEscapableString
import de.dkfz.roddy.execution.BashInterpreter
import de.dkfz.roddy.execution.ConcatenatedString
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.tools.shell.bash.Service
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.EMPTY
import static de.dkfz.roddy.execution.EscapableString.*

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
    GridEngineBasedSubmissionCommand(BatchEuphoriaJobManager parentJobManager,
                                     BEJob job,
                                     AnyEscapableString jobName,
                                     List<ProcessingParameters> processingParameters,
                                     Map<String, AnyEscapableString> environmentVariables) {
        super(parentJobManager, job, jobName, processingParameters, environmentVariables)
    }


    @Override
    protected AnyEscapableString assembleDependencyParameter(List<BEJobID> jobIds) {
        AnyEscapableString qsubCall = c()
        LinkedList<AnyEscapableString> tempDependencies =
                jobIds.findAll {
                    it.id != "" && it.id != NONE && it.id != "-1"
                }.collect {
                    e(it.id.split("\\.")[0])
                    // Keep the command line short. GE accepts the job number for dependencies.
                } as LinkedList<AnyEscapableString>
        if (tempDependencies.size() > 0) {
            qsubCall = join([qsubCall,
                             u(getDependsSuperParameter()),
                             u(getDependencyParameterName()),
                             u(getDependencyOptionSeparator())
                            ],
                            getDependencyIDSeparator())
        }

        qsubCall
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
    protected String composeCommandString(List<AnyEscapableString> parameters) {
        StringBuilder command = new StringBuilder(EMPTY)

        if (job.code) {
            command <<
            "echo -ne " <<
            BashInterpreter.instance.interpret(e(job.code)) <<
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
            ConcatenatedString commandToBeExecuted = join(job.command, " ")
            if (quoteCommand) {
                command += u(" ") + e(commandToBeExecuted)
            } else {
                command += u(" ") + commandToBeExecuted
            }
        }

        return command
    }
}
