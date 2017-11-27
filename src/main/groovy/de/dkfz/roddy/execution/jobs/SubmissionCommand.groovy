/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */
package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.JobLog
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.EMPTY

@CompileStatic
abstract class SubmissionCommand extends Command {
    /**
     * A command to be executed on the cluster head node, in particular qsub, bsub, qstat, etc.
     *
     * @param parentJobManager
     * @param job
     * @param jobName
     * @param parameters Useful, if the set of parameters used for the execution command is not identical to the Job's parameters.
     *
     */
    protected SubmissionCommand(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName, Map<String, String> parameters) {
        super(parentJobManager, job, jobName, parameters)
    }

    @Override
    String toString() {

        String email = parentJobManager.getUserEmail()
        String umask = parentJobManager.getUserMask()
        String groupList = parentJobManager.getUserGroup()
        String accountName = job.customUserAccount ?: parentJobManager.getUserAccount()
        boolean holdJobsOnStart = parentJobManager.isHoldJobsEnabled()

        // collect parameters for qsub
        List<String> parameters = []
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
        parameters << assembleVariableExportString()
        parameters << getAdditionalCommandParameters()


        // create qsub call
        StringBuilder command = new StringBuilder(EMPTY)

        if (job.getToolScript()) {
            command << "echo " << escapeBash(job.getToolScript()) << " | "
        }

        command << getSubmissionCommand()
        command << " ${parameters.join(" ")} "

        if (job.getTool()) {
            command << " " << job.getTool().getAbsolutePath()
        }

        return command
    }

    abstract protected String getSubmissionCommand()

    abstract protected String getJobNameParameter()
    abstract protected String getHoldParameter()
    abstract protected String getAccountParameter(String account)
    abstract protected String getWorkingDirectory()
    abstract protected String getLoggingParameter(JobLog jobLog)
    abstract protected String getEmailParameter(String address)
    abstract protected String getGroupListParameter(String groupList)
    abstract protected String getUmaskString(String umask)
    abstract protected String assembleDependencyString(List<BEJobID> jobIds)
    abstract protected String assembleVariableExportString()
    abstract protected String getAdditionalCommandParameters()//?


    String assembleProcessingCommands() {
        StringBuilder bsubCall = new StringBuilder()
        for (ProcessingParameters pcmd in job.getListOfProcessingParameters()) {
            if (!(pcmd instanceof ProcessingParameters)) continue
            ProcessingParameters command = (ProcessingParameters) pcmd
            if (command == null)
                continue
            bsubCall << StringConstants.WHITESPACE << command.getProcessingCommandString()
        }
        return bsubCall.toString()
    }
}
