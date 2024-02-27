/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ)..
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.AnyEscapableString
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedJobManager
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic

import static de.dkfz.roddy.execution.EscapableString.*

@CompileStatic
class PBSJobManager extends GridEngineBasedJobManager<PBSSubmissionCommand> {

    PBSJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    @Override
    PBSSubmissionCommand createCommand(BEJob job) {
        return new PBSSubmissionCommand(
                this, job, u(job.jobName), [], job.parameters)
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
    void createComputeParameter(ResourceSet resourceSet,
                                LinkedHashMultimap<String, AnyEscapableString> parameters) {
        int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
        int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1
        // Currently not active
        String enforceSubmissionNodes = ''
        if (!enforceSubmissionNodes) {
            String pVal = 'nodes=' << nodes << ':ppn=' << cores
            if (resourceSet.isAdditionalNodeFlagSet()) {
                pVal << ':' << resourceSet.getAdditionalNodeFlag()
            }
            parameters.put("-l", u(pVal))
        } else {
            String[] nodesArr = enforceSubmissionNodes.split(StringConstants.SPLIT_SEMICOLON)
            nodesArr.each {
                String node ->
                    parameters.put('-l',
                                   u('nodes=') + node +
                                   ':ppn=' + resourceSet.getCores().toString())
            }
        }
    }

    void createQueueParameter(LinkedHashMultimap<String, AnyEscapableString> parameters,
                              String queue) {
        parameters.put('-q', e(queue))
    }

    @Override
    void createWalltimeParameter(LinkedHashMultimap<String, AnyEscapableString> parameters,
                                 ResourceSet resourceSet) {
        parameters.put('-l', u('walltime=') + TimeUnit.fromDuration(resourceSet.walltime).toString())
    }

    @Override
    void createMemoryParameter(LinkedHashMultimap<String, AnyEscapableString> parameters,
                               ResourceSet resourceSet) {
        parameters.put('-l', u('mem=') + resourceSet.getMem().toString(BufferUnit.M))
    }

    @Override
    void createStorageParameters(LinkedHashMultimap<String, AnyEscapableString> parameters,
                                 ResourceSet resourceSet) {
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
