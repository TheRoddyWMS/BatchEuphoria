/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.ClusterJobManager
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedJobManager
import de.dkfz.roddy.tools.*
import groovy.util.slurpersupport.GPathResult

import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.regex.Matcher

/**
 * @author michael
 */
@groovy.transform.CompileStatic
class PBSJobManager extends GridEngineBasedJobManager<PBSCommand> {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(PBSJobManager)

    PBSJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    @Override
    boolean checkCompatibilityToHost() {
        // PBS tests are non trivial. qsub and co are not very helpful, as they don't feature any useful messages.
        // As we support PBS Torque, we'll check installation folders and for a specific started service.
        // Checking directories of course can miss, that Torque is installed in a different directory which does
        // not contain "torque".
        String cmd = "which qsub;"
        cmd += "ps -u root | grep trqauthd >/dev/null && echo PBS || NONPBS" // Append fast check for running torque auth daemon.
        def result = executionService.execute(cmd)

        // We should have two lines:
        //   /opt/torque/bin/qsub
        //   PBS / NONPBS
        return result.resultLines.size() == 2 &&
                result.resultLines[0].contains("torque") &&
                result.resultLines[1] == "PBS"
    }

    @Override
    protected PBSCommand createCommand(BEJob job) {
        return new PBSCommand(this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id, job.tool?.getAbsolutePath() ?: job.getToolScript())
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String commandString) {
        return new PBSCommandParser(commandString).toGenericJobInfo();
    }

    /**
     * Return the position of the status string within a stat result line. This changes if -u USERNAME is used!
     *
     * @return
     */
    protected int getColumnOfJobState() {
        if (isTrackingOfUserJobsEnabled)
            return 9
        return 4
    }

    @Override
    String parseJobID(String commandOutput) {
        return commandOutput
    }

    @Override
    protected JobState parseJobState(String stateString) {
        // http://docs.adaptivecomputing.com/torque/6-1-0/adminGuide/help.htm#topics/torque/commands/qstat.htm
        JobState js = JobState.UNKNOWN
        if (stateString == "R")
            js = JobState.RUNNING
        if (stateString == "H")
            js = JobState.HOLD
        if (stateString == "S")
            js = JobState.SUSPENDED
        if (stateString in ["Q", "T", "W"])
            js = JobState.QUEUED
        if (stateString in ["C", "E"]) {
            js = JobState.COMPLETED_UNKNOWN
        }

        return js;
    }

    @Override
    List<String> getEnvironmentVariableGlobs() {
        return Collections.unmodifiableList(["PBS_*"])
    }

    @Override
    String getQueryJobStatesCommand() {
        return "qstat -t"
    }

    @Override
    String getExtendedQueryJobStatesCommand() {
        return "qstat -x -f"
    }

    @Override
    void createComputeParameter(ResourceSet resourceSet, LinkedHashMultimap<String, String> parameters) {
        int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
        int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1
        // Currently not active
        String enforceSubmissionNodes = ''
        if (!enforceSubmissionNodes) {
            String pVal = 'nodes=' << nodes << ':ppn=' << cores
            if (resourceSet.isAdditionalNodeFlagSet()) {
                pVal << ':' << resourceSet.getAdditionalNodeFlag()
            }
            parameters.put("-l", pVal)
        } else {
            String[] nodesArr = enforceSubmissionNodes.split(StringConstants.SPLIT_SEMICOLON)
            nodesArr.each {
                String node ->
                    parameters.put('-l', 'nodes=' + node + ':ppn=' + resourceSet.getCores())
            }
        }
    }

    void createQueueParameter(LinkedHashMultimap<String, String> parameters, String queue) {
        parameters.put('-q', queue)
    }

    @Override
    void createWalltimeParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put('-l', 'walltime=' + TimeUnit.fromDuration(resourceSet.walltime))
    }

    @Override
    void createMemoryParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put('-l', 'mem=' + resourceSet.getMem().toString(BufferUnit.M))
    }

    @Override
    void createStorageParameters(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
    }

    @Override
    String getJobIdVariable() {
        return "PBS_JOBID"
    }

    @Override
    String getJobNameVariable() {
        return "PBS_JOBNAME"
    }

    @Override
    String getQueueVariable() {
        return 'PBS_QUEUE'
    }

    @Override
    String getNodeFileVariable() {
        return "PBS_NODEFILE"
    }

    @Override
    String getSubmitHostVariable() {
        return "PBS_O_HOST"
    }

    @Override
    String getSubmitDirectoryVariable() {
        return "PBS_O_WORKDIR"
    }

}
