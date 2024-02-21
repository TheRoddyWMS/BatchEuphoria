/*
 * Copyright (c) 2022 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ)..
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.slurm

import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.CommandI
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedSubmissionCommand
import de.dkfz.roddy.tools.BashUtils
import de.dkfz.roddy.tools.shell.bash.Service
import groovy.transform.CompileStatic
import org.apache.commons.text.StringEscapeUtils

import static de.dkfz.roddy.StringConstants.*

@CompileStatic
class SlurmSubmissionCommand extends GridEngineBasedSubmissionCommand {

    public static final String NONE = "none"
    public static final String AFTEROK = "afterok"
    public static final String PARM_DEPENDS = " --dependency="

    SlurmSubmissionCommand(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName,
                           List<ProcessingParameters> processingParameters, Map<String, String> environmentVariables,
                           List<String> dependencyIDs) {
        super(parentJobManager, job, jobName, processingParameters, environmentVariables, dependencyIDs)
    }

    @Override
    String getSubmissionExecutableName() {
        return "sbatch"
    }

    @Override
    protected String getJobNameParameter() {
        return "--job-name ${jobName}" as String
    }

    @Override
    protected String getHoldParameter() {
        return "--hold"
    }

    @Override
    protected String getAccountNameParameter() {
        return job.accountingName != null ? "--account=\"${job.accountingName}\"" : ""
    }

    @Override
    protected String getWorkingDirectoryParameter() {
        return "--chdir ${job.getWorkingDirectory() ?: WORKING_DIRECTORY_DEFAULT}" as String
    }

    @Override
    protected String getLoggingParameter(JobLog jobLog) {
        if (!jobLog.out && !jobLog.error) {
            return ""
        } else if (jobLog.out == jobLog.error) {
            return "--output=${jobLog.out.replace(JobLog.JOB_ID, '%j')}"
        } else {
            return "--output=${jobLog.out.replace(JobLog.JOB_ID, '%j')} --error=${jobLog.error.replace(JobLog.JOB_ID, '%j')}"
        }
    }

    @Override
    protected String getEmailParameter(String address) {
        return address ? " --mail-user=" + address : ""
    }

    protected String getParsableParameter() {
        return "--parsable"
    }

    @Override
    protected String getGroupListParameter(String groupList) {
        return " --grid=" + groupList
    }

    @Override
    protected String getUmaskString(String umask) {
        return ""
    }

    @Override
    String getDependencyParameterName() {
        return AFTEROK
    }

    /**
     * In this case i.e. afterokarray:...,afterok:
     * A comma
     * @return
     */
    @Override
    protected String getDependencyOptionSeparator() {
        return ":"
    }

    @Override
    protected String getDependencyIDSeparator() {
        return COLON
    }

    @Override
    protected String getAdditionalCommandParameters() {
        return "${getParsableParameter()} --kill-on-invalid-dep=yes --propagate=none" as String
    }

    @Override
    protected String getEnvironmentString() {
        return ""
    }

    @Override
    String assembleVariableExportParameters() {
        List<String> parameterStrings = []

        if (passLocalEnvironment)
            parameterStrings << "--get-user-env "

        List<String> environmentStrings = parameters.collect { key, value ->
            if (null == value)
                "${key}"
            else
                "${key}=${value}"
        } as List<String>

        if (!environmentStrings.empty)
            parameterStrings << "--export=\"${environmentStrings.join(COMMA)}\"".toString()

        return parameterStrings.join(WHITESPACE)
    }

    protected String getDependsSuperParameter() {
        PARM_DEPENDS
    }

    @Override
    protected String composeCommandString(List<String> parameters) {
        StringBuilder command = new StringBuilder(EMPTY)

        if (job.code) {
            command <<
            "echo -ne " <<
            // SLURM must have a shebang line for the job script.
            escapeScriptForEval(job.code) <<
            " | "
        }

        if (environmentString) {
            command << "$environmentString "
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
        } else if (job.code) {
            // SLURM can only read scripts from files, not from stdin.
            command << " /dev/stdin"
        }

        return command
    }
}
