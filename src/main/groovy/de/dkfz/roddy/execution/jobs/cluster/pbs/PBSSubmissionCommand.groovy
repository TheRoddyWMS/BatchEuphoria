/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ)..
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedSubmissionCommand
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.COLON
import static de.dkfz.roddy.StringConstants.COMMA
import static de.dkfz.roddy.StringConstants.EMPTY
import static de.dkfz.roddy.StringConstants.WHITESPACE

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

    PBSSubmissionCommand(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName,
                         List<ProcessingParameters> processingParameters, Map<String, String> environmentVariables,
                         List<String> dependencyIDs, String command) {
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
    protected String getAccountNameParameter() {
        return job.accountingName != null ? "-A \"${job.accountingName}\"" : ""
    }

    @Override
    protected String getWorkingDirectoryParameter() {
        return "-w ${job.getWorkingDirectory() ?: WORKING_DIRECTORY_DEFAULT}" as String
    }

    @Override
    protected String getLoggingParameter(JobLog jobLog) {
        if (!jobLog.out && !jobLog.error) {
            return "-k n"
        } else if (jobLog.out == jobLog.error) {
            return "${joinLogParameter} -o \"${jobLog.out.replace(JobLog.JOB_ID, '\\$PBS_JOBID')}\""
        } else {
            return "-o \"${jobLog.out.replace(JobLog.JOB_ID, '\\$PBS_JOBID')} -e ${jobLog.error.replace(JobLog.JOB_ID, '\\$PBS_JOBID')}\""
        }
    }

    @Override
    protected String getEmailParameter(String address) {
        return address ? " -M " + address : ""
    }

    protected String getJoinLogParameter() {
        return "-j oe"
    }

    @Override
    protected String getGroupListParameter(String groupList) {
        return " -W group_list=" + groupList
    }

    @Override
    protected String getUmaskString(String umask) {
        return umask ? "-W umask=" + umask : ""
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
        return COLON
    }

    @Override
    protected String getDependencyIDSeparator() {
        return COLON
    }

    @Override
    protected String getAdditionalCommandParameters() {
        return EMPTY
    }

    @Override
    protected String getEnvironmentString() {
        return ""
    }

    @Override
    String assembleVariableExportParameters() {
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
            parameterStrings << "-v \"${environmentStrings.join(COMMA)}\"".toString()

        return parameterStrings.join(WHITESPACE)
    }

    protected String getDependsSuperParameter() {
        PARM_DEPENDS
    }
}
