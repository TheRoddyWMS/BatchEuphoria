/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.EMPTY

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.SubmissionCommand

/**
 * This class is used to create and execute bsub commands.
 */
@CompileStatic
class LSFSubmissionCommand extends SubmissionCommand {

    LSFSubmissionCommand(
            BatchEuphoriaJobManager parentJobManager,
            BEJob job,
            String jobName,
            List<ProcessingParameters> processingParameters,
            Map<String, String> environmentVariables,
            List<String> dependencyIDs,
            String command) {
        super(parentJobManager, job, jobName, processingParameters, environmentVariables, dependencyIDs, command)
    }

    @Override
    protected String getJobNameParameter() {
        return "-J ${jobName}" as String
    }

    @Override
    protected String getHoldParameter() {
        return "-H"
    }

    @Override
    protected String getAccountingNameParameter() {
        return job.accountingName != null ? "-P \"${job.accountingName}\"" : ""
    }

    @Override
    protected String getAccountParameter(String account) {
        return EMPTY
    }

    @Override
    protected String getWorkingDirectoryParameter() {
        return "-cwd ${job.workingDirectory ?: WORKING_DIRECTORY_DEFAULT}" as String
    }

    @Override
    protected String getLoggingParameter(JobLog jobLog) {
        return getLoggingParameters(jobLog)
    }

    static getLoggingParameters(JobLog jobLog) {
        if (!jobLog.out && !jobLog.error) {
            return "-o /dev/null" //always set logging because it interacts with mail options
        } else if (jobLog.out == jobLog.error) {
            return "-oo ${jobLog.out.replace(JobLog.JOB_ID, '%J')}"
        } else {
            return "-oo ${jobLog.out.replace(JobLog.JOB_ID, '%J')} -eo ${jobLog.error.replace(JobLog.JOB_ID, '%J')}"
        }
    }

    @Override
    protected String getEmailParameter(String address) {
        return address ? "-u ${address}" : ""
    }

    @Override
    String getGroupListParameter(String groupList) {
        return "-G" + groupList
    }

    @Override
    protected String getUmaskString(String umask) {
        return EMPTY
    }

    @Override
    protected String assembleDependencyParameter(List<BEJobID> jobIds) {
        List<BEJobID> validJobIds = BEJob.uniqueValidJobIDs(jobIds)
        if (validJobIds.size() > 0) {
            String joinedParentJobs = validJobIds.collect { "done(${it})" }.join(" && ")
            // -ti: Immediate orphan job termination for jobs with failed dependencies.
            return "-ti -w  \"${joinedParentJobs}\" "
        } else {
            return EMPTY
        }
    }

    @Override
    protected String getAdditionalCommandParameters() {
        return EMPTY
    }

    @Override
    protected String getEnvironmentString() {
        return LSFJobManager.getEnvironmentString()
    }

    // TODO Code duplication with PBSCommand. Check also DirectSynchronousCommand.
    /**
     *  Note that variable quoting is left to the client code. The whole -env parameter-value is quoted with ".
     *
     * * @return    a String of '{-env {"none", "{all|}(, varName[=varValue](, varName[=varValue])*|}"'
     */
    @Override
    String assembleVariableExportParameters() {

        List<String> environmentStrings = parameters.collect { key, value ->
            if (null == value)
                "${key}"              // returning just the variable name makes bsub take the value form the bsub-commands execution environment
            else
                "${key}=${value}"     // sets value to value
        } as List<String>

        if (passLocalEnvironment) {
            environmentStrings = ["all"] + environmentStrings
        } else if (parameters.isEmpty()) {
            environmentStrings += "none"
        }

        return "-env \"${environmentStrings.join(", ")}\""
    }

}
