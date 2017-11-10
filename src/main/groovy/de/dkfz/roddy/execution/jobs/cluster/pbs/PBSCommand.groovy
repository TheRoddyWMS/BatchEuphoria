/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.tools.LoggerWrapper
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import java.util.logging.Level

import static de.dkfz.roddy.StringConstants.COLON
import static de.dkfz.roddy.StringConstants.EMPTY

/**
 * This class is used to create and execute qsub commands
 *
 * @author michael
 */
@groovy.transform.CompileStatic
class PBSCommand extends Command {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(PBSCommand.class.name)

    public static final String NONE = "none"
    public static final String NONE_ARR = "none[]"
    public static final String AFTEROK = "afterok"
    public static final String AFTEROK_ARR = "afterokarray"
    public static final String QSUB = "qsub"
    public static final String PARM_ACCOUNT = " -A "
    public static final String PARM_JOBNAME = " -N "
    public static final String PARM_JOINLOGS = " -j oe"
    public static final String PARM_LOGPATH = " -o "
    public static final String PARM_MAIL = " -M "
    public static final String PARM_GROUPLIST = " -W group_list="
    public static final String PARM_UMASK = " -W umask="
    public static final String PARM_DEPENDS = " -W depend="
    public static final String PARM_VARIABLES = " -v "
    public static final String PARM_WRAPPED_SCRIPT = "WRAPPED_SCRIPT="

    /**
     * The qsub log directoy where all output is put
     */
    protected File loggingDirectory

    /**
     * The command which should be called
     */
    protected String command

    /**
     * Provide a lower and upper array index to make this qsub job an array job
     */
    protected List<String> arrayIndices

    protected List<String> dependencyIDs

    protected final List<ProcessingParameters> processingParameters

    /**
     *
     * @param id
     * @param parameters
     * @param arrayIndices
     * @param command
     * @param filesToCheck
     */
    PBSCommand(PBSJobManager parentManager, BEJob job, String id, List<ProcessingParameters> processingParameters, Map<String, String> parameters, Map<String, Object> tags, List<String> arrayIndices, List<String> dependencyIDs, String command, File loggingDirectory) {
        super(parentManager, job, id, parameters, tags)
        this.processingParameters = processingParameters
        this.command = command
        this.loggingDirectory = loggingDirectory
        this.arrayIndices = arrayIndices ?: new LinkedList<String>()
        this.dependencyIDs = dependencyIDs ?: new LinkedList<String>()
    }

    boolean getIsArray() {
        return arrayIndices != null && arrayIndices.size() > 0
    }

    String getJoinLogParameter() {
        return PARM_JOINLOGS
    }

    String getEmailParameter(String address) {
        return PARM_MAIL + address
    }

    String getGroupListString(String groupList) {
        return PARM_GROUPLIST + groupList
    }

    String getUmaskString(String umask) {
        return PARM_UMASK + umask
    }

    String getDependencyParameterName() {
        return AFTEROK
    }

    String getVariablesParameter() {
        return PARM_VARIABLES
    }

    /**
     * In this case i.e. afterokarray:...,afterok:
     * A comma
     * @return
     */
    protected String getDependencyTypesSeparator() {

    }

    protected String getDependencyOptionSeparator() {
        return COLON
    }

    protected String getDependencyIDSeparator() {
        return COLON
    }

    protected String getArrayDependencyParameterName() {
        return AFTEROK_ARR
    }

    protected String getAdditionalCommandParameters() {
        return ""
    }

    protected String getDependsSuperParameter() {
        PARM_DEPENDS
    }

    @Override
    String toString() {

        String email = parentJobManager.getUserEmail()
        String umask = parentJobManager.getUserMask()
        String groupList = parentJobManager.getUserGroup()
        String accountName = job.customUserAccount ?: parentJobManager.getUserAccount()
        boolean holdJobsOnStart = parentJobManager.isHoldJobsEnabled()

        StringBuilder qsubCall = new StringBuilder(EMPTY)

        if (job.getToolScript()) {
            qsubCall << "echo " << escapeBash(job.getToolScript()) << " | "
        }

        qsubCall << QSUB << PARM_JOBNAME << id

        if (holdJobsOnStart) qsubCall << " -h "

        if (accountName) qsubCall << PARM_ACCOUNT << accountName << " "

        qsubCall << getAdditionalCommandParameters()

        qsubCall << "-w " << getWorkingDirectory()

        qsubCall << getLoggingParameter(job.jobLog)

        if (isArray) qsubCall << assembleArraySettings()

        if (email) qsubCall << getEmailParameter(email)

        if (groupList && groupList != "UNDEFINED") qsubCall << getGroupListString(groupList)

        if (umask) qsubCall << getUmaskString(umask)

        qsubCall << assembleProcessingCommands()

        qsubCall << assembleDependencyString()

        qsubCall << assembleVariableExportString()

        if (job.getTool()) {
            qsubCall << " " << job.getTool().getAbsolutePath()
        }

        return qsubCall
    }

    StringBuilder assembleArraySettings() {
        StringBuilder qsubCall = new StringBuilder(" -t ")
        StringBuilder sbArrayIndices = new StringBuilder("")
        //TODO Make a second list of array indices, which is valid for job submission. The current translation with the help of counting is not optimal!
        int i = 1 //TODO Think if pbs arrays should always start with one?
        for (String ai in arrayIndices) {
            if (ai.isNumber())
                sbArrayIndices << ai.toInteger()
            else
                sbArrayIndices << i
            sbArrayIndices << StringConstants.COMMA
            i++
        }
        qsubCall << sbArrayIndices.toString()[0..-2]
        return qsubCall
    }

    // TODO Code duplication with PBSCommand. Check also DirectSynchronousCommand.
    String assembleVariableExportString() {
        StringBuilder qsubCall = new StringBuilder()

        if (job.parameters.containsKey("CONFIG_FILE") && job.parameters.containsKey("PARAMETER_FILE")) {
            qsubCall << getVariablesParameter() << "\"" << "CONFIG_FILE=" << job.parameters["CONFIG_FILE"] << ",PARAMETER_FILE=" << job.parameters["PARAMETER_FILE"]

            if (job.parameters.containsKey("debugWrapInScript")) {
                qsubCall << "," << "debugWrapInScript=" << job.parameters["debugWrapInScript"]
            }
            qsubCall << "\""

        } else {
            qsubCall << getVariablesParameter() << "\"" << parameters.collect { key, value -> "${key}=${value}" }.join(",") << "\""
        }

        return qsubCall
    }

    String assembleDependencyString() {
        StringBuilder qsubCall = new StringBuilder("")
        LinkedList<String> tempDependencies =
                creatingJob.getParentJobIDs().findAll {
                    it.getId() != "" && it.getId() != NONE && it.getId() != "-1"
                }.collect {
                    it.getId().split("\\.")[0] // Keep the command line short. PBS accepts the job number for dependencies.
                } as LinkedList<String>
        if (creatingJob.getParentJobIDs().any { it.getId().contains("[].") }) {
            throw new NotImplementedException()
        }
        if (tempDependencies.size() > 0) {
            try {
                qsubCall <<
                        getDependsSuperParameter() <<
                        getDependencyParameterName() <<
                        getDependencyOptionSeparator() <<
                        tempDependencies.join(getDependencyIDSeparator())
            } catch (Exception ex) {
                logger.log(Level.SEVERE, ex.toString())
            }
        }

        return qsubCall
    }

    private String getLoggingParameter(JobLog jobLog) {
        if (!jobLog.out && !jobLog.error) {
            return "-k"
        } else if (!jobLog.error) {
            return "${getJoinLogParameter()} -o ${jobLog.out.replace(JobLog.JOB_ID, '$PBS_JOBID')}"
        } else {
            return "-o ${jobLog.out.replace(JobLog.JOB_ID, '$PBS_JOBID')} -e ${jobLog.error.replace(JobLog.JOB_ID, '$PBS_JOBID')}"
        }
    }
}
