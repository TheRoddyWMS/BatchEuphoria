/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.sge

import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.SubmissionCommand
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedCommand
import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSJobManager
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.COLON
import static de.dkfz.roddy.StringConstants.COMMA
import static de.dkfz.roddy.StringConstants.EMPTY
import static de.dkfz.roddy.StringConstants.WHITESPACE

/**
 * Created by michael on 20.05.14.
 */
@CompileStatic
class SGECommand extends GridEngineBasedCommand {

    SGECommand(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName, List<ProcessingParameters> processingParameters, Map<String, String> environmentVariables, List<String> dependencyIDs, String command) {
        super(parentJobManager, job, jobName, processingParameters, environmentVariables, dependencyIDs, command)
    }

    @Override
    protected String getJobNameParameter() {
        return "-N ${jobName}" as String
    }

    @Override
    protected String getHoldParameter() {
        return "-h"
    }

    @Override
    protected String getAccountParameter(String account) {
        return account ? "-A ${account}" as String : ""
    }

    @Override
    protected String getWorkingDirectory() {
        return "-wd ${job.getWorkingDirectory() ?: WORKING_DIRECTORY_DEFAULT}" as String
    }

    @Override
    protected String getLoggingParameter(JobLog jobLog) {
        if (!jobLog.out && !jobLog.error) {
            return "-o /dev/null -e /dev/null"
        } else if (jobLog.out == jobLog.error) {
            return "${joinLogParameter} -o \"${jobLog.out.replace(JobLog.JOB_ID, '\\$JOB_ID')}\""
        } else {
            return "-o \"${jobLog.out.replace(JobLog.JOB_ID, '\\$JOB_ID')} -e ${jobLog.error.replace(JobLog.JOB_ID, '\\$JOB_ID')}\""
        }
    }

    @Override
    protected String getEmailParameter(String address) {
        return address ? " -M " + address : ""
    }

    protected String getJoinLogParameter() {
        return "-j y"
    }

    @Override
    protected String getGroupListParameter(String groupList) {
        return EMPTY
    }

    @Override
    protected String getUmaskString(String umask) {
        return WHITESPACE
    }

    @Override
    String getDependencyParameterName() {
        return "-hold_jid"
    }

    @Override
    String getDependencyOptionSeparator() {
        return WHITESPACE
    }

    @Override
    String getDependencyIDSeparator() {
        return COMMA
    }

    @Override
    protected String getAdditionalCommandParameters() {
        return " -S /bin/bash "
    }

    @Override
    protected String getEnvironmentString() {
        return ""
    }

    @Override
    protected String assembleVariableExportParameters() {
        List<String> parameterStrings = []

        if (passLocalEnvironment)
            parameterStrings << "-V"

        List<String> environmentStrings = parameters.collect { key, value ->
            if (null == value)
                "${key}"              // returning just the variable name makes qsub take the value form the qsub-commands execution environment
            else
                "${key}=${value}"     // sets value to value
        } as List<String>

        if (!environmentStrings.empty)
            parameterStrings << "-v \"${environmentStrings.join(",")}\"".toString()

        return parameterStrings.join(" ")
    }

    protected String getDependsSuperParameter() {
        return WHITESPACE
    }
}
