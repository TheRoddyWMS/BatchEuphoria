/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.SubmissionCommand
import de.dkfz.roddy.tools.LoggerWrapper

/**
 * This class is used to create and execute bsub commands
 *
 * Created by kaercher on 12.04.17.
 */
@groovy.transform.CompileStatic
class LSFCommand extends SubmissionCommand {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(LSFCommand.class.name)

    /**
     * The command which should be called
     */
    protected String command

    protected List<String> dependencyIDs

    protected final List<ProcessingParameters> processingParameters

    /**
     *
     * @param name
     * @param parameters
     * @param command
     * @param filesToCheck
     */
    LSFCommand(LSFJobManager parentManager, BEJob job, String name, List<ProcessingParameters> processingParameters, Map<String, String> parameters, List<String> dependencyIDs, String command) {
        super(parentManager, job, name, parameters)
        this.processingParameters = processingParameters
        this.command = command
        this.dependencyIDs = dependencyIDs ?: new LinkedList<String>()
    }

    @Override
    protected String getEnvironmentExportParameter() {
        // TODO Is this really not supported?
        return ""
    }

    @Override
    String getJobNameParameter() {
        "-J ${jobName}" as String
    }

    @Override
    protected String getHoldParameter() {
        return "-H"
    }

    @Override
    protected String getAccountParameter(String account) {
        return ""
    }

    @Override
    protected String getWorkingDirectory() {
        return "-cwd ${job.getWorkingDirectory() ?: WORKING_DIRECTORY_DEFAULT}" as String
    }

    @Override
    protected String getLoggingParameter(JobLog jobLog) {
        getLoggingParameters(jobLog)
    }

    static getLoggingParameters(JobLog jobLog) {
        if (!jobLog.out && !jobLog.error) {
            return "-o /dev/null" //always set logging because it interacts with mail options
        } else if (jobLog.out == jobLog.error) {
            return "-oo ${jobLog.out.replace(JobLog.JOB_ID, '%J')}"
        } else {
            return "-oo ${jobLog.out.replace(JobLog.JOB_ID, '%J')} -eo ${jobLog.error.replace(JobLog.JOB_ID, '%J')}"
        }
    }

    @Override
    protected String getEmailParameter(String address) {
        return address ? "-u ${address}" : ""
    }

    @Override
    String getGroupListParameter(String groupList) {
        return " -G" + groupList
    }

    @Override
    protected String getUmaskString(String umask) {
        return ""
    }

    @Override
    protected String assembleDependencyString(List<BEJobID> jobIds) {
        List<BEJobID> validJobIds = BEJob.uniqueValidJobIDs(jobIds)
        if (validJobIds.size() > 0) {
            String joinedParentJobs = validJobIds.collect { "done(${it})" }.join(" && ")
            // -ti: Immediate orphan job termination for jobs with failed dependencies.
            return "-ti -w  \"${joinedParentJobs}\" "
        } else {
            return ""
        }
    }


    // TODO Code duplication with PBSCommand. Check also DirectSynchronousCommand.
    /**
     * Compose the -env parameter of bsub. This supports the 'none' and 'all' parameters to clean the bsub environment or to copy the full
     * environment during submission to the execution host. This depends on whether the `passEnvironment` field is set (default=true). Also
     * copying specific variables (just use variable name pointing to null value in the `parameters` hash) and setting to a specific value are
     * supported.
     *
     * Note that variable quoting is left to the client code. The whole -env parameter-value is quoted with ".
     *
     * * @return    a String of '-env "[all|none](, varName[=varValue](, varName[=varValue])*)?"'
     */
    @Override
    String assembleVariableExportString() {
        StringBuilder qsubCall = new StringBuilder()
        qsubCall << "-env " << "\""
        if (passLocalEnvironment) {
            qsubCall << "all"
        } else {
            qsubCall << "none"
        }

        if (job.parameters.containsKey("CONFIG_FILE") && job.parameters.containsKey("PARAMETER_FILE")) {
            // This code is exclusively meant to quickfix Roddy. Remove this branch if Roddy is fixed.
            // WARNING: Note the additional space before the parameter delimiter! It is necessary for bsub -env but must not be there for qsub in PBS!
            qsubCall << ", CONFIG_FILE=" << job.parameters["CONFIG_FILE"] << ", PARAMETER_FILE=" << job.parameters["PARAMETER_FILE"]

            if (job.parameters.containsKey("debugWrapInScript")) {
                qsubCall << ", " << "debugWrapInScript=" << job.parameters["debugWrapInScript"]
            }
        } else {
            if (!parameters.isEmpty()) {
                qsubCall << ", " << parameters.collect { key, value ->
                    if (null == value)
                        key                   // returning just the variable name make bsub take the value form the *bsub-commands* execution environment
                    else
                        "${key}=${value}"     // sets value to value
                }.join(", ")
            }
        }

        qsubCall << "\""

        return qsubCall
    }

    protected String getAdditionalCommandParameters() {
        ""
    }
}
