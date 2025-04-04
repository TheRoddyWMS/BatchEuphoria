/*
 * Copyright (c) 2022 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ)..
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.slurm


import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.tools.EscapableString
import de.dkfz.roddy.tools.BashInterpreter
import de.dkfz.roddy.tools.ConcatenatedString
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedSubmissionCommand
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.COLON
import static de.dkfz.roddy.StringConstants.COMMA
import static de.dkfz.roddy.tools.EscapableString.Shortcuts.*

@CompileStatic
class SlurmSubmissionCommand extends GridEngineBasedSubmissionCommand {

    public static final String NONE = "none"
    public static final String AFTEROK = "afterok"
    public static final String PARM_DEPENDS = " --dependency="

    SlurmSubmissionCommand(BatchEuphoriaJobManager parentJobManager, BEJob job,
                           EscapableString jobName,
                           List<ProcessingParameters> processingParameters,
                           Map<String, EscapableString> environmentVariables) {
        super(parentJobManager, job, jobName, processingParameters, environmentVariables)
    }

    @Override
    String getSubmissionExecutableName() {
        return "sbatch"
    }

    @Override
    protected EscapableString getJobNameParameter() {
        u("--job-name ") + jobName
    }

    @Override
    protected EscapableString getHoldParameter() {
        u("--hold")
    }

    @Override
    protected EscapableString getAccountNameParameter() {
        job.accountingName != null ? u("--account=") + job.accountingName : c()
    }

    @Override
    protected EscapableString getWorkingDirectoryParameter() {
        ConcatenatedString result = c(u("--chdir "))
        if (job.workingDirectory) {
            // The workingDirectory is a File object. So no variables (such as $HOME) are supported.
            result += e(job.workingDirectory.toString())
        } else {
            // The $HOME will be enclosed in double quotes, but not escaped. The variable should
            // be expanded on the call-site.
            result += u('"') + WORKING_DIRECTORY_DEFAULT + u('"')
        }
        return result
   }

    @Override
    protected EscapableString getLoggingParameter(JobLog jobLog) {
        if (!jobLog.out && !jobLog.error) {
            c()
        } else if (jobLog.out == jobLog.error) {
            u("--output=") + e(jobLog.out.replace(JobLog.JOB_ID, '%j'))
        } else {
            u("--output=") + e(jobLog.out.replace(JobLog.JOB_ID, '%j')) +
                u(" --error=") + e(jobLog.error.replace(JobLog.JOB_ID, '%j'))
        }
    }

    @Override
    protected EscapableString getEmailParameter(EscapableString address) {
        address ? u(" --mail-user=") + address : c()
    }

    protected EscapableString getParsableParameter() {
        u("--parsable")
    }

    @Override
    protected EscapableString getGroupListParameter(EscapableString groupList) {
        u(" --grid=") + groupList
    }

    @Override
    protected EscapableString getUmaskString(EscapableString umask) {
        c()
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
    protected EscapableString getAdditionalCommandParameters() {
        u(parsableParameter) + u(" --kill-on-invalid-dep=yes --propagate=NONE")
    }

    @Override
    protected EscapableString getEnvironmentString() {
        c()
    }

    @Override
    EscapableString assembleVariableExportParameters() {
        ConcatenatedString parameterStrings = c()

        if (passLocalEnvironment)
            parameterStrings += u("--get-user-env ")

        List<EscapableString> environmentStrings =
                parameters.collect { key, value ->
                    if (null == value)
                        u(key)
                    else
                        u(key) + e("=") + value
                } as List<EscapableString>

        if (!environmentStrings.empty)
            parameterStrings += u("--export=") + join(environmentStrings, u(COMMA))
        return parameterStrings
    }

    protected String getDependsSuperParameter() {
        PARM_DEPENDS
    }

    @Override
    protected String composeCommandString(List<EscapableString> parameters) {
        ConcatenatedString command = c()

        if (job.code) {
            // SLURM must have a shebang line for the job script.
            // Note that we escape the code once.
            command += u("echo -ne ") + e(job.code) + u(" | ")
        }

        if (environmentString != null && environmentString != c()) {
            command += environmentString + u(" ")
        }

        command += u(submissionExecutableName)

        command += c(u(" "), join(parameters, u(" ")), u(" "))

        if (job.command) {
            ConcatenatedString commandToBeExecuted = join(job.command, " ")
            if (quoteCommand) {
                command += u(" ") + e(commandToBeExecuted)
            } else {
                command += u(" ") + commandToBeExecuted
            }
        } else if (job.code) {
            // SLURM can only read scripts from files, not from stdin.
            command += " /dev/stdin"
        }

        return BashInterpreter.instance.interpret(command)
    }
}
