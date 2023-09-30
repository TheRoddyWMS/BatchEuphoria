/*
 * Copyright (c) 2023 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */
package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.CommandI
import de.dkfz.roddy.tools.BashUtils
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.EMPTY

@CompileStatic
abstract class SubmissionCommand extends BECommand {

    /**
     *  Should the local environment during the submission be copied to the execution hosts?
     *  This is an Optional, because the actual value will be calculated from both the Job/Command configuration and
     *  the JobManager.
     */
    Optional<Boolean> passEnvironment = Optional.empty()

    /**
     * The command which should be called
     */
    protected CommandI commandObj

    protected List<String> dependencyIDs

    protected final List<ProcessingParameters> processingParameters

    /**
     * A command to be executed on the cluster head node, in particular qsub, bsub, qstat, etc.
     *
     * @param parentJobManager
     * @param job
     * @param jobName
     * @param processingParameters
     * @param environmentVariables
     * @param dependencyIDs
     * @param commandObj
     *
     * Unfortunately, *all* these parameters can be null.
     */
    protected SubmissionCommand(BatchEuphoriaJobManager parentJobManager,
                                BEJob job,
                                String jobName,
                                List<ProcessingParameters> processingParameters,
                                Map<String, String> environmentVariables,
                                List<String> dependencyIDs,
                                CommandI commandObj) {
        super(parentJobManager, job, jobName, environmentVariables)
        this.processingParameters = processingParameters
        this.commandObj = commandObj
        this.dependencyIDs = dependencyIDs ?: new LinkedList<String>()
    }

    /**
     * Should the local environment be passed?
     *
     * JobManager and SubmissionCommand together determine, whether the environment should be passed.
     *
     * * The Command has precedence over the JobManager value.
     * * If the Command value is not set, the JobManager value is the fallback.
     * * If neither JobManager nor Command are defined, only copy the requested variables to remote.
     *
     * @return
     */
    Boolean getPassLocalEnvironment() {
        passEnvironment.orElse(parentJobManager.passEnvironment)
    }

    @Override
    String toBashCommandString() {
        String email = parentJobManager.getUserEmail()
        String umask = parentJobManager.getUserMask()
        String groupList = parentJobManager.getUserGroup()
        boolean holdJobsOnStart = parentJobManager.isHoldJobsEnabled()

        // collect parameters for job submission
        List<String> parameters = []
        parameters << assembleVariableExportParameters()
        parameters << getAccountNameParameter()
        parameters << getJobNameParameter()
        if (holdJobsOnStart) parameters << getHoldParameter()
        parameters << getWorkingDirectoryParameter()
        parameters << getLoggingParameter(job.jobLog)
        parameters << getEmailParameter(email)
        if (groupList && groupList != "UNDEFINED") parameters << getGroupListParameter(groupList)
        parameters << getUmaskString(umask)
        parameters << assembleProcessingCommands()
        parameters << assembleDependencyParameter(creatingJob.parentJobIDs)
        parameters << getAdditionalCommandParameters()

        // create job submission command call
        StringBuilder command = new StringBuilder(EMPTY)

        if (environmentString) {
            command << "$environmentString "
        }

        if (job.code) {
            command <<
                    "echo " <<
                    BashUtils.strongQuote("#!/bin/bash "
                                          + System.lineSeparator()
                                          + job.code) <<
                    " | "
        }

        command << parentJobManager.submissionCommand
        command << " ${parameters.join(" ")} "

        if (job.getCommand(true)) {
            command << " " << job.getCommand(true).join(StringConstants.WHITESPACE)
        }

        return command
    }

    abstract protected String getJobNameParameter()

    abstract protected String getHoldParameter()

    protected String getAccountNameParameter() {
        return ""
    }

    abstract protected String getWorkingDirectoryParameter()

    abstract protected String getLoggingParameter(JobLog jobLog)

    abstract protected String getEmailParameter(String address)

    abstract protected String getGroupListParameter(String groupList)

    abstract protected String getUmaskString(String umask)

    abstract protected String assembleDependencyParameter(List<BEJobID> jobIds)

    abstract protected String getAdditionalCommandParameters()

    abstract protected String getEnvironmentString()

    /** If passLocalEnvironment is true, all local variables will be forwarded to the execution host.
     *  If passLocalEnvironment is false, no local variables will be forwarded by default.
     *  In both cases arbitrary variables can be set to specific values or be declared to be forwarded as defined in the local environment (according
     *  to the parameters field; null-value parameters are copied as locally defined).
     *
     * @return A set of parameters for the submission command to achieve the requested variable exports.
     */
    abstract protected String assembleVariableExportParameters()

    String assembleProcessingCommands() {
        StringBuilder commands = new StringBuilder()
        for (ProcessingParameters pcmd in job.getListOfProcessingParameters()) {
            if (!(pcmd instanceof ProcessingParameters)) continue
            ProcessingParameters command = (ProcessingParameters) pcmd
            if (command == null)
                continue
            commands << StringConstants.WHITESPACE << command.getProcessingCommandString()
        }
        return commands.toString()
    }
}
