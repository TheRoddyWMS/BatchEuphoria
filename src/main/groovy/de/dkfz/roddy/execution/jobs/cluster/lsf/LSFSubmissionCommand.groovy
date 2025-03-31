/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf


import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.tools.EscapableString
import de.dkfz.roddy.tools.BashInterpreter
import de.dkfz.roddy.tools.ConcatenatedString
import de.dkfz.roddy.execution.jobs.*
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.EMPTY
import static de.dkfz.roddy.tools.EscapableString.Shortcuts.*

/**
 * This class is used to create and execute bsub commands.
 */
@CompileStatic
class LSFSubmissionCommand extends SubmissionCommand {

    LSFSubmissionCommand(
            BatchEuphoriaJobManager parentJobManager,
            BEJob job,
            EscapableString jobName,
            List<ProcessingParameters> processingParameters,
            Map<String, EscapableString> environmentVariables) {
        super(parentJobManager, job, jobName, processingParameters, environmentVariables)
    }

    @Override
    String getSubmissionExecutableName() {
        return "bsub"
    }

    @Override
    protected EscapableString getJobNameParameter() {
        u("-J ") + jobName
    }

    @Override
    protected EscapableString getHoldParameter() {
        u("-H")
    }

    @Override
    protected EscapableString getAccountNameParameter() {
        job.accountingName != null ?
            u("-P ") + job.accountingName :
            u("")
    }

    @Override
    protected EscapableString getWorkingDirectoryParameter() {
        ConcatenatedString result = c(u("-cwd "))
        if (job.workingDirectory) {
            // The workingDirectory is a File object. So no variables (such as $HOME) are supported.
            result += e(job.workingDirectory.toString())
        } else {
            // The $HOME will be quoted with double quotes, but not escaped. The variable should
            // be expanded on the call-site.
            result += u('"') + WORKING_DIRECTORY_DEFAULT + u('"')
        }
        return result
    }

    @Override
    protected EscapableString getLoggingParameter(JobLog jobLog) {
        getLoggingParameters(jobLog)
    }

    @Override
    protected Boolean getQuoteCommand() {
        true
    }

    static EscapableString getLoggingParameters(JobLog jobLog) {
        if (!jobLog.out && !jobLog.error) {
            u("-o /dev/null") //always set logging because it interacts with mail options
        } else if (jobLog.out == jobLog.error) {
            u("-oo ") + e(jobLog.out.replace(JobLog.JOB_ID, '%J'))
        } else {
            u( "-oo ") + e(jobLog.out.replace(JobLog.JOB_ID, '%J')) +
            u(" -eo ") + e(jobLog.error.replace(JobLog.JOB_ID, '%J'))
        }
    }

    @Override
    protected EscapableString getEmailParameter(EscapableString address) {
        address ? u("-u ") + address : u("")
    }

    @Override
    EscapableString getGroupListParameter(EscapableString groupList) {
        u("-G ") + groupList
    }

    @Override
    protected EscapableString getUmaskString(EscapableString umask) {
        u(EMPTY)
    }

    @Override
    protected EscapableString assembleDependencyParameter(List<BEJobID> jobIds) {
        List<BEJobID> validJobIds = BEJob.uniqueValidJobIDs(jobIds)
        if (validJobIds.size() > 0) {
            EscapableString joinedParentJobs =
                    join(validJobIds.collect {
                        u("done(${it})")
                    } as List<EscapableString>, u(" && "))

            // -ti: Immediate orphan job termination for jobs with failed dependencies.
            u("-ti -w  ") + e(joinedParentJobs)
        } else {
            c()
        }
    }

    @Override
    protected EscapableString getAdditionalCommandParameters() {
        u(EMPTY)
    }

    @Override
    protected EscapableString getEnvironmentString() {
        LSFJobManager.environmentString
    }

    // TODO Code duplication with PBSCommand. Check also DirectSynchronousCommand.
    /**
     *  Note that variable quoting is left to the client code. The whole -env parameter-value is quoted with ".
     *
     * * @return    a String of '{-env {"none", "{all|}(, varName[=varValue](, varName[=varValue])*|}"'
     */
    @Override
    EscapableString assembleVariableExportParameters() {

        List<EscapableString> environmentStrings = parameters.collect { key, value ->
            if (null == value)
                // Returning just the variable name lets bsub take the value from the bsub-commands execution environment
                u(key)
            else
                // Set value to value
                u(key) + u("=") + value
        } as List<EscapableString>

        if (passLocalEnvironment) {
            environmentStrings = ([u("all")] as List<EscapableString>) + environmentStrings
        } else if (parameters.isEmpty()) {
            environmentStrings = [u("none")] as List<EscapableString>
        }

        return u("-env") + " " + join(environmentStrings, e(", "))
    }

    @Override
    protected String composeCommandString(List<EscapableString> parameters) {
        ConcatenatedString command = c()

        if (job.code) {
            // LSF can just read the script to execute from the standard input.
            command += u("echo -ne ") + e(job.code) + u(" | ")
        }

        if (environmentString) {
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
        }

        return BashInterpreter.instance.interpret(command)
    }

}
