/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.tools.LoggerWrapper

import java.util.regex.Matcher

import static de.dkfz.roddy.StringConstants.COLON
import static de.dkfz.roddy.StringConstants.EMPTY

/**
 * This class is used to create and execute bsub commands
 *
 * Created by kaercher on 12.04.17.
 */
@groovy.transform.CompileStatic
class LSFCommand extends Command {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(LSFCommand.class.name)

    public static final String PARM_LOGPATH = " -eo "
    public static final String PARM_OUTPATH = " -oo "
    public static final String BSUB = "bsub"
    public static final String PARM_DEPENDS = " -ti -w "
    public static final String PARM_MAIL = " -u "
    public static final String PARM_VARIABLES = " -env "
    public static final String PARM_JOBNAME = " -J "
    public static final String PARM_GROUPLIST = " -G"

    /**
     * The bsub log directoy where all output is put
     */
    protected File loggingDirectory

    /**
     * The command which should be called
     */
    protected String command

    protected List<String> dependencyIDs

    /**
     *
     */
    public boolean copyExecutionEnvironment = true

    protected final List<ProcessingParameters> processingParameters

    /**
     *
     * @param id
     * @param parameters
     * @param arrayIndices
     * @param command
     * @param filesToCheck
     */
    LSFCommand(LSFJobManager parentManager, BEJob job, String id, List<ProcessingParameters> processingParameters, Map<String, String> parameters, Map<String, Object> tags, List<String> arrayIndices, List<String> dependencyIDs, String command, File loggingDirectory) {
        super(parentManager, job, id, parameters, tags)
        this.processingParameters = processingParameters
        this.command = command
        assert (null != loggingDirectory)
        this.loggingDirectory = loggingDirectory
        this.dependencyIDs = dependencyIDs ?: new LinkedList<String>()
    }


    String getEmailParameter(String address) {

        return PARM_MAIL + address

    }

    String getLoggingParameter() {
        StringBuilder logging = new StringBuilder(EMPTY)
        logging << (PARM_OUTPATH + " ${loggingDirectory}/${id ? id : "%J"}.o%J ")
        logging << (PARM_LOGPATH + " ${loggingDirectory}/${id ? id : "%J"}.e%J ")
        return logging
    }

    String getGroupListString(String groupList) {
        return PARM_GROUPLIST + groupList
    }


    String getVariablesParameter() {
        return PARM_VARIABLES
    }

    protected String getDependencyOptionSeparator() {
        return COLON
    }

    protected String getDependencyIDSeparator() {
        return COLON
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
        String accountName = parentJobManager.getUserAccount()
        boolean holdJobsOnStart = parentJobManager.isHoldJobsEnabled()

        StringBuilder bsubCall = new StringBuilder(EMPTY)

        if (job.getToolScript()) {
            bsubCall << "echo '" << job.getToolScript().replaceAll("'", Matcher.quoteReplacement("'\\''")) << "' | "
        }

        bsubCall << BSUB << assembleResources() << PARM_JOBNAME << id

        if (holdJobsOnStart) bsubCall << " -H "

        bsubCall << getAdditionalCommandParameters()

        if (loggingDirectory) bsubCall << getLoggingParameter()

        if (email) bsubCall << getEmailParameter(email)

        if (groupList && groupList != "UNDEFINED") bsubCall << getGroupListString(groupList)

        bsubCall << assembleProcessingCommands()

        bsubCall << prepareParentJobs(job.getParentJobIDs())

        bsubCall << assembleVariableExportString()

        if (job.getTool()) {
            bsubCall << " " << job.getTool().getAbsolutePath()
        }

        return bsubCall
    }


    StringBuilder assembleResources() {
        StringBuilder resources = new StringBuilder(" -R \'select[type==any] ")
        if (job.resourceSet.isCoresSet()) {
            int cores = job.resourceSet.isCoresSet() ? job.resourceSet.getCores() : 1
            resources.append(" affinity[core(${cores})]")
        }
        resources.append("\' ")
        return resources
    }

    // TODO Code duplication with PBSCommand. Check also DirectSynchronousCommand.
    /**
     * Compose the -env parameter of bsub. This supports the 'none' and 'all' parameters to clean the bsub environment or to copy the full
     * environment during submission to the execution host. This depends on whether the `copyExecutionEnvironment` field is set (default=true). Also
     * copying specific variables (just use variable name pointing to null value in the `parameters` hash) and setting to a specific value are
     * supported.
     *
     * Note that variable quoting is left to the client code. The whole -env parameter-value is quoted with ".
     *
     * * @return    a String of '-env "[all|none](, varName[=varValue](, varName[=varValue])*)?"'
     */
    String assembleVariableExportString() {
        StringBuilder qsubCall = new StringBuilder()
        qsubCall << getVariablesParameter() << "\""
        if (copyExecutionEnvironment) {
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


    /**
     * Prepare parent jobs is part of @prepareExtraParams
     * @param job ids
     * @return part of parameter area
     */
    private String prepareParentJobs(List<BEJobID> jobIds) {
        List<BEJobID> validJobIds = BEJob.uniqueValidJobIDs(jobIds)
        if (validJobIds.size() > 0) {
            String joinedParentJobs = validJobIds.collect { "done(${it})" }.join(" && ")
            return " ${dependsSuperParameter} \"${joinedParentJobs}\" "
        } else {
            return ""
        }
    }


    private String prepareToolScript(BEJob job) {
        String toolScript
        if (job.getToolScript() != null && job.getToolScript().length() > 0) {
            toolScript = job.getToolScript()
        } else {
            if (job.getTool() != null) toolScript = job.getTool().getAbsolutePath()
        }
        if (toolScript) {
            return toolScript
        } else {
            return ""
        }
    }

}
