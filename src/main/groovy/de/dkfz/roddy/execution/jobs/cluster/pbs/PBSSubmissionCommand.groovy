/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ)..
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.tools.EscapableString
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedSubmissionCommand
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.*
import static de.dkfz.roddy.tools.EscapableString.Shortcuts.*

/**
 * This class is used to create and execute qsub commands
 *
 * @author michael
 */
@CompileStatic
class PBSSubmissionCommand extends GridEngineBasedSubmissionCommand {

    public static final String NONE = "none"
    public static final String AFTEROK = "afterok"
    public static final String PARM_DEPENDS = " -W depend="

    PBSSubmissionCommand(BatchEuphoriaJobManager parentJobManager,
                         BEJob job, EscapableString jobName,
                         List<ProcessingParameters> processingParameters,
                         Map<String, EscapableString> environmentVariables) {
        super(parentJobManager, job, jobName, processingParameters, environmentVariables)
    }

    @Override
    protected EscapableString getJobNameParameter() {
        u("-N ") + jobName
    }

    @Override
    protected EscapableString getHoldParameter() {
        u("-h")
    }

    @Override
    protected EscapableString getAccountNameParameter() {
        job.accountingName != null ? u("-A ") + job.accountingName : c()
    }

    @Override
    protected EscapableString getWorkingDirectoryParameter() {
        u("-w ") + e(job.getWorkingDirectory().toString()) ?: WORKING_DIRECTORY_DEFAULT
    }

    @Override
    protected EscapableString getLoggingParameter(JobLog jobLog) {
        if (!jobLog.out && !jobLog.error) {
            u("-k n")
        } else if (jobLog.out == jobLog.error) {
            joinLogParameter + u("-o ") + e(jobLog.out.replace(JobLog.JOB_ID, '\\$PBS_JOBID'))
        } else {
            join([
                    u("-o"),
                    e(jobLog.out.replace(JobLog.JOB_ID, '\\$PBS_JOBID')),
                    u("-e"),
                    e(jobLog.error.replace(JobLog.JOB_ID, '\\$PBS_JOBID'))
                ], WHITESPACE)
        }
    }

    @Override
    protected EscapableString getEmailParameter(EscapableString address) {
        return address ? u(" -M ") + address : c()
    }

    protected EscapableString getJoinLogParameter() {
        u("-j oe")
    }

    @Override
    protected EscapableString getGroupListParameter(EscapableString groupList) {
        u(" -W group_list=") + groupList
    }

    @Override
    protected EscapableString getUmaskString(EscapableString umask) {
        umask ? u("-W umask=") + umask : c()
    }

    @Override
    String getDependencyParameterName() {
        AFTEROK
    }

    /**
     * In this case i.e. afterokarray:...,afterok:
     * A comma
     * @return
     */
    @Override
    protected String getDependencyOptionSeparator() {
        return COLON
    }

    @Override
    protected String getDependencyIDSeparator() {
        return COLON
    }

    @Override
    protected EscapableString getAdditionalCommandParameters() {
        u(EMPTY)
    }

    @Override
    protected EscapableString getEnvironmentString() {
        c()
    }

    @Override
    EscapableString assembleVariableExportParameters() {

        List<EscapableString> environmentStrings = parameters.collect { key, value ->
            if (null == value)
                // Returning just the variable name makes qsub take the value form the qsub-commands execution environment
                u(key)
            else
                // Sets value to value
                u(key) + e("=") + value
        } as List<EscapableString>

        List<EscapableString> parameterStrings = []

        if (passLocalEnvironment)
            parameterStrings << u("-V")

        if (!environmentStrings.empty)
            parameterStrings << u("-v ") + join(environmentStrings, COMMA)

        return join(parameterStrings, " ")
    }

    protected String getDependsSuperParameter() {
        PARM_DEPENDS
    }
}
