/*
 * Copyright (c) 2023 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */
package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.tools.EscapableString
import groovy.transform.CompileStatic

import static de.dkfz.roddy.tools.EscapableString.Shortcuts.*

@CompileStatic
abstract class SubmissionCommand extends Command {

    /**
     *  Should the local environment during the submission be copied to the execution hosts?
     *  This is an Optional, because the actual value will be calculated from both the Job/Command configuration and
     *  the JobManager.
     */
    Optional<Boolean> passEnvironment = Optional.empty()

    protected final List<ProcessingParameters> processingParameters

    /**
     * A command to be executed on the cluster head node, in particular qsub, bsub, qstat, etc.
     *
     * @param parentJobManager
     * @param job
     * @param jobName
     * @param processingParameters
     * @param environmentVariables
     *
     * Unfortunately, *all* these parameters can be null.
     */
    protected SubmissionCommand(BatchEuphoriaJobManager parentJobManager,
                                BEJob job,
                                EscapableString jobName,
                                List<ProcessingParameters> processingParameters,
                                Map<String, EscapableString> environmentVariables) {
        super(parentJobManager, job, jobName, environmentVariables)
        this.processingParameters = processingParameters
    }

    protected abstract Boolean getQuoteCommand()

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

    abstract String getSubmissionExecutableName()

    private List<EscapableString> collectParameters() {
        String email = parentJobManager.getUserEmail()
        String umask = parentJobManager.getUserMask()
        String groupList = parentJobManager.getUserGroup()
        boolean holdJobsOnStart = parentJobManager.isHoldJobsEnabled()

        // collect parameters for job submission
        List<EscapableString> parameters = []
        parameters << assembleVariableExportParameters()
        parameters << getAccountNameParameter()
        parameters << getJobNameParameter()
        if (holdJobsOnStart) parameters << getHoldParameter()
        parameters << getWorkingDirectoryParameter()
        parameters << getLoggingParameter(job.jobLog)
        if (email) parameters << getEmailParameter(e(email))
        if (groupList && groupList != "UNDEFINED") parameters << getGroupListParameter(e(groupList))
        if (umask) parameters << getUmaskString(e(umask))
        parameters << assembleProcessingCommands()
        parameters << assembleDependencyParameter(creatingJob.parentJobIDs)
        parameters << getAdditionalCommandParameters()

        parameters
    }

    abstract protected String composeCommandString(List<EscapableString> parameters)

    @Override
    String toBashCommandString() {
        return composeCommandString(collectParameters())
    }

    abstract protected EscapableString getJobNameParameter()

    abstract protected EscapableString getHoldParameter()

    protected EscapableString getAccountNameParameter() {
        return c()
    }

    abstract protected EscapableString getWorkingDirectoryParameter()

    abstract protected EscapableString getLoggingParameter(JobLog jobLog)

    abstract protected EscapableString getEmailParameter(EscapableString address)

    abstract protected EscapableString getGroupListParameter(EscapableString groupList)

    abstract protected EscapableString getUmaskString(EscapableString umask)

    abstract protected EscapableString assembleDependencyParameter(List<BEJobID> jobIds)

    abstract protected EscapableString getAdditionalCommandParameters()

    abstract protected EscapableString getEnvironmentString()

    /** If passLocalEnvironment is true, all local variables will be forwarded to the execution host.
     *  If passLocalEnvironment is false, no local variables will be forwarded by default.
     *  In both cases arbitrary variables can be set to specific values or be declared to be forwarded as defined in the local environment (according
     *  to the parameters field; null-value parameters are copied as locally defined).
     *
     * @return A set of parameters for the submission command to achieve the requested variable exports.
     */
    abstract protected EscapableString assembleVariableExportParameters()

    EscapableString assembleProcessingCommands() {
        EscapableString commands = c()
        for (ProcessingParameters pcmd in job.getListOfProcessingParameters()) {
            if (pcmd instanceof ProcessingParameters) {
                ProcessingParameters command = (ProcessingParameters) pcmd
                if (command != null)
                    commands += u(StringConstants.WHITESPACE) + command.getProcessingCommandString()
            }
        }
        commands
    }

}
