/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.sge

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedJobManager
import de.dkfz.roddy.tools.*

/**
 * @author michael
 */
@groovy.transform.CompileStatic
class SGEJobManager extends GridEngineBasedJobManager<SGESubmissionCommand> {

    SGEJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    @Override
    protected SGESubmissionCommand createCommand(BEJob job) {
        return new SGESubmissionCommand(this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id, job.command)
    }

    @Override
    String getQueryJobStatesCommand() {
        return "qstat -g d -j"
    }

    @Override
    String getExtendedQueryJobStatesCommand() {
        return "qstat -xml -ext -f -j"
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String command) {
        return null
    }

    @Override
    String parseJobID(String commandOutput) {
        if (!commandOutput.startsWith("Your job"))
            return null
        String id = commandOutput.split(StringConstants.SPLIT_WHITESPACE)[2]
        return id
    }

    @Override
    protected JobState parseJobState(String stateString) {
        //TODO: add all combinations, see http://www.softpanorama.org/HPC/Grid_engine/Queues/queue_states.shtml
        JobState js = JobState.UNKNOWN
        if (stateString == "r")
            js = JobState.RUNNING
        if (stateString == "hqw")
            js = JobState.HOLD
        if (stateString == "S")
            js = JobState.SUSPENDED
        if (stateString in ["qw", "T", "W"])
            js = JobState.QUEUED
        if (stateString in ["C", "E"]) {
            js = JobState.COMPLETED_UNKNOWN
        }

        return js;
    }

    @Override
    void createComputeParameter(ResourceSet resourceSet, LinkedHashMultimap<String, String> parameters) {
        parameters.put("-pe", "serial ${resourceSet.cores}")
    }

    void createQueueParameter(LinkedHashMultimap<String, String> parameters, String queue) {
        parameters.put('-q', queue)
    }

    @Override
    void createWalltimeParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put("-l", "h_rt=${TimeUnit.fromDuration(resourceSet.walltime).toHourString()}")
    }

    @Override
    void createMemoryParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put("-l", "h_rss=${resourceSet.getMem().toString(BufferUnit.M)}")
    }

    @Override
    void createStorageParameters(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
    }

    @Override
    String getJobIdVariable() {
        return "JOBID"
    }

    @Override
    String getJobNameVariable() {
        return "JOB_NAME"
    }

    @Override
    String getQueueVariable() {
        return null
    }

    @Override
    String getNodeFileVariable() {
        return null
    }

    @Override
    String getSubmitHostVariable() {
        return null
    }

    @Override
    String getSubmitDirectoryVariable() {
        return null
    }

}
