/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ)..
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.sge

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.AnyEscapableString
import de.dkfz.roddy.execution.CommandI
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedSubmissionCommand
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.COMMA
import static de.dkfz.roddy.StringConstants.WHITESPACE
import static de.dkfz.roddy.execution.EscapableString.*

/**
 * Created by michael on 20.05.14.
 */
@CompileStatic
class SGESubmissionCommand extends GridEngineBasedSubmissionCommand {

    SGESubmissionCommand(BatchEuphoriaJobManager parentJobManager,
                         BEJob job, AnyEscapableString jobName,
                         List<ProcessingParameters> processingParameters,
                         Map<String, AnyEscapableString> environmentVariables) {
        super(parentJobManager, job, jobName, processingParameters, environmentVariables)
    }

    @Override
    protected AnyEscapableString getJobNameParameter() {
        u("-N ") + jobName
    }

    @Override
    protected AnyEscapableString getHoldParameter() {
        u("-h")
    }

    @Override
    protected AnyEscapableString getAccountNameParameter() {
        job.accountingName != null ? u("-A ") + job.accountingName : c()
    }

    @Override
    protected AnyEscapableString getWorkingDirectoryParameter() {
        u("-wd ") + e(job.getWorkingDirectory().toString()) ?: WORKING_DIRECTORY_DEFAULT
    }

    @Override
    protected AnyEscapableString getLoggingParameter(JobLog jobLog) {
        if (!jobLog.out && !jobLog.error) {
            u("-o /dev/null -e /dev/null")
        } else if (jobLog.out == jobLog.error) {
            joinLogParameter + u(" -o ") + e(jobLog.out.replace(JobLog.JOB_ID, '\\$JOB_ID'))
        } else {
            join([
                         u("-o"),
                         e(jobLog.out.replace(JobLog.JOB_ID, '\\$JOB_ID')),
                         u("-e"),
                         e(jobLog.error.replace(JobLog.JOB_ID, '\\$JOB_ID'))
                ], " ")
        }
    }

    @Override
    protected AnyEscapableString getEmailParameter(AnyEscapableString address) {
        address ? u(" -M ") + address : c()
    }

    protected AnyEscapableString getJoinLogParameter() {
        u("-j y")
    }

    @Override
    protected AnyEscapableString getGroupListParameter(AnyEscapableString groupList) {
        c()
    }

    @Override
    protected AnyEscapableString getUmaskString(AnyEscapableString umask) {
        u(WHITESPACE)
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
    protected AnyEscapableString getAdditionalCommandParameters() {
        u(" -S /bin/bash ")
    }

    @Override
    protected AnyEscapableString getEnvironmentString() {
        c()
    }

    @Override
    protected AnyEscapableString assembleVariableExportParameters() {
        List<AnyEscapableString> parameterStrings = []

        if (passLocalEnvironment)
            parameterStrings += u("-V")

        List<String> environmentStrings = parameters.collect { key, value ->
            if (null == value)
                u(key) // returning just the variable name makes qsub take the value form the qsub-commands execution environment
            else
                u("$key=") + value     // sets value to value
        } as List<String>

        if (!environmentStrings.empty)
            parameterStrings += u("-v ") + e(environmentStrings.join(","))

        join(parameterStrings, " ")
    }

    protected String getDependsSuperParameter() {
        return WHITESPACE
    }

}
