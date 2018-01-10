import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.SubmissionCommand

/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

class SlurmCommand extends SubmissionCommand {

    /**
     * A command to be executed on the cluster head node, in particular qsub, bsub, qstat, etc.
     *
     * @param parentJobManager
     * @param job
     * @param jobName
     * @param parameters Useful, if the set of parameters used for the execution command is not identical to the Job's parameters.
     *
     */
    protected SlurmCommand(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName, Map<String, String> parameters) {
        super(parentJobManager, job, jobName, parameters)
    }

    @Override
    protected String getEnvironmentExportParameter() {
        return null
    }

    @Override
    protected String getJobNameParameter() {
        return null
    }

    @Override
    protected String getHoldParameter() {
        return null
    }

    @Override
    protected String getAccountParameter(String account) {
        return null
    }

    @Override
    protected String getWorkingDirectory() {
        return null
    }

    @Override
    protected String getLoggingParameter(JobLog jobLog) {
        return null
    }

    @Override
    protected String getEmailParameter(String address) {
        return null
    }

    @Override
    protected String getGroupListParameter(String groupList) {
        return null
    }

    @Override
    protected String getUmaskString(String umask) {
        return null
    }

    @Override
    protected String assembleDependencyString(List<BEJobID> jobIds) {
        return null
    }

    @Override
    protected String assembleVariableExportString() {
        return null
    }

    @Override
    protected String getAdditionalCommandParameters() {
        return null
    }
}