/*
 * Copyright (c) 2018 German Cancer Research Center (DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.slurm

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.SubmissionCommand
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedCommand
import groovy.transform.CompileStatic

@CompileStatic
class SlurmCommand extends GridEngineBasedCommand {

    /**
     * A command to be executed on the cluster head node, in particular qsub, bsub, qstat, etc.
     *
     * @param parentJobManager
     * @param job
     * @param jobName
     * @param processingParameters @param environmentVariables @param dependencyIDs @param command
     */
    SlurmCommand(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName, List<ProcessingParameters> processingParameters, Map<String, String> environmentVariables, List<String> dependencyIDs, String command) {
        super(parentJobManager, job, jobName, processingParameters, environmentVariables, dependencyIDs, command)
    }

//    submit = """
//        sbatch -J ${job_name} -D ${cwd} -o ${out} -e ${err} -t ${runtime_minutes} -p ${queue} \
//        ${"-n " + cpus} \
//        --mem-per-cpu=${requested_memory_mb_per_core} \
//        --wrap "/bin/bash ${script}"
//    """
//    kill = "scancel ${job_id}"
//    check-alive = "squeue -j ${job_id}"
//    job-id-regex = "Submitted batch job (\\d+).*"

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
    protected String getAdditionalCommandParameters() {
        return null
    }

    @Override
    protected String getEnvironmentString() {
        return ""
    }

    @Override
    protected String assembleVariableExportParameters() {
        return null
    }

    @Override
    protected String getDependsSuperParameter() {
        return null
    }

    @Override
    protected String getDependencyParameterName() {
        return null
    }

    @Override
    protected String getDependencyOptionSeparator() {
        return null
    }

    @Override
    protected String getDependencyIDSeparator() {
        return null
    }
}
