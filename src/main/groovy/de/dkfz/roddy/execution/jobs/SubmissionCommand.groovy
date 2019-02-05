/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */
package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.tools.BashUtils
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.EMPTY

@CompileStatic
abstract class SubmissionCommand extends Command {

    /**
     *  Should the local environment during the submission be copied to the execution hosts?
     *  This is an Optional, because the actual value will be calculated from both the Job/Command comfiguration and the JobManager.
     */
    Optional<Boolean> passEnvironment = Optional.empty()

    /**
     * The command which should be called
     */
    protected String command

    protected List<String> dependencyIDs

    protected final List<ProcessingParameters> processingParameters
    /**
     * A command to be executed on the cluster head node, in particular qsub, bsub, qstat, etc.
     *
     * @param parentJobManager
     * @param job
     * @param jobName
     * @param environmentVariables
     *
     */
    protected SubmissionCommand(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName, List<ProcessingParameters> processingParameters,
                                Map<String, String> environmentVariables, List<String> dependencyIDs, String command) {
        super(parentJobManager, job, jobName, environmentVariables)
        this.processingParameters = processingParameters
        this.command = command
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
        String accountName = job.customUserAccount ?: parentJobManager.getUserAccount()
        boolean holdJobsOnStart = parentJobManager.isHoldJobsEnabled()

        // collect parameters for job submission
        List<String> parameters = []
        parameters << assembleVariableExportParameters()
        parameters << getJobNameParameter()
        if (holdJobsOnStart) parameters << getHoldParameter()
        parameters << getAccountParameter(accountName)
        parameters << getWorkingDirectory()
        parameters << getLoggingParameter(job.jobLog)
        parameters << getEmailParameter(email)
        if (groupList && groupList != "UNDEFINED") parameters << getGroupListParameter(groupList)
        parameters << getUmaskString(umask)
        parameters << assembleProcessingCommands()
        parameters << assembleDependencyString(creatingJob.parentJobIDs)
        parameters << getAdditionalCommandParameters()

        // create job submission command call
        StringBuilder command = new StringBuilder(EMPTY)

        if (job.getToolScript()) {
            command << "echo " << BashUtils.strongQuote(job.getToolScript()) << " | "
        }

        command << parentJobManager.getSubmissionCommand()
        command << " ${parameters.join(" ")} "

        if (job.getTool()) {
            command << " " << job.getTool().getAbsolutePath()
        }

        return command
    }

    abstract protected String getJobNameParameter()

    abstract protected String getHoldParameter()

    abstract protected String getAccountParameter(String account)

    abstract protected String getWorkingDirectory()

    abstract protected String getLoggingParameter(JobLog jobLog)

    abstract protected String getEmailParameter(String address)

    abstract protected String getGroupListParameter(String groupList)

    abstract protected String getUmaskString(String umask)

    abstract protected String assembleDependencyString(List<BEJobID> jobIds)

    abstract protected String getAdditionalCommandParameters()

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
