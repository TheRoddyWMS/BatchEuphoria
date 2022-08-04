/*
 * Copyright (c) 2022 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ)..
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.slurm

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedSubmissionCommand
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.*

@CompileStatic
class SlurmSubmissionCommand extends GridEngineBasedSubmissionCommand {

    public static final String NONE = "none"
    public static final String AFTEROK = "afterok"
    public static final String PARM_DEPENDS = " --dependency="

    SlurmSubmissionCommand(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName,
                           List<ProcessingParameters> processingParameters, Map<String, String> environmentVariables,
                           List<String> dependencyIDs, String command) {
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
        return "--job-name ${jobName}" as String
    }

    @Override
    protected String getHoldParameter() {
        return "--hold"
    }

    @Override
    protected String getAccountNameParameter() {
        return job.accountingName != null ? "--account=\"${job.accountingName}\"" : ""
    }

    @Override
    protected String getWorkingDirectoryParameter() {
        return "--chdir ${job.getWorkingDirectory() ?: WORKING_DIRECTORY_DEFAULT}" as String
    }

    @Override
    protected String getLoggingParameter(JobLog jobLog) {
        if (!jobLog.out && !jobLog.error) {
            return ""
        } else if (jobLog.out == jobLog.error) {
            return "--output=${jobLog.out.replace(JobLog.JOB_ID, '%j') + "/slurm-%j.out"}"
        } else {
            return "--output=${jobLog.out.replace(JobLog.JOB_ID, '%j') + "/slurm-%j.out"} --error=${jobLog.error.replace(JobLog.JOB_ID, '%j') + "/slurm-%j.out"}"
        }
    }

    @Override
    protected String getEmailParameter(String address) {
        return address ? " --mail-user=" + address : ""
    }

    protected String getParsableParameter() {
        return "--parsable"
    }

    @Override
    protected String getGroupListParameter(String groupList) {
        return " --grid=" + groupList
    }

    @Override
    protected String getUmaskString(String umask) {
        return ""
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
        return ":"
    }

    @Override
    protected String getDependencyIDSeparator() {
        return COLON
    }

    @Override
    protected String getAdditionalCommandParameters() {
        return getParsableParameter()
    }

    @Override
    protected String getEnvironmentString() {
        return ""
    }

    @Override
    String assembleVariableExportParameters() {
        List<String> parameterStrings = []

        if (passLocalEnvironment)
            parameterStrings << "--get-user-env "

        List<String> environmentStrings = parameters.collect { key, value ->
            if (null == value)
                "${key}"
            else
                "${key}=${value}"
        } as List<String>

        if (!environmentStrings.empty)
            parameterStrings << "--export=\"${environmentStrings.join(COMMA)}\"".toString()

        return parameterStrings.join(WHITESPACE)
    }

    protected String getDependsSuperParameter() {
        PARM_DEPENDS
    }
}
