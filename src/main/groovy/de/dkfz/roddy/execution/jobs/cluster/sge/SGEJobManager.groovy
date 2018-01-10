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
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSJobManager
import de.dkfz.roddy.tools.BufferUnit
import groovy.transform.CompileStatic

/**
 * Created by michael on 20.05.14.
 */
@CompileStatic
class SGEJobManager extends PBSJobManager {
    SGEJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
        parms.getAdditionalOptions()
    }

    @Override
    protected SGECommand createCommand(BEJob job) {
        return new SGECommand(this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id, job.tool?.getAbsolutePath() ?: job.getToolScript(), null)
    }

    @Override
    protected int getPositionOfJobID() {
        return 0
    }

    @Override
    protected int getPositionOfJobState() {
        return 4
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

    @Deprecated
    protected List<String> getTestQstat() {
        return Arrays.asList(
                "job - ID prior name user jobState submit / start at queue slots ja -task - ID",
                "---------------------------------------------------------------------------------------------------------------- -",
                "   1187 0.75000 r140710_09 seqware r 07 / 10 / 2014 09:51:55 main.q @worker3 1",
                "   1188 0.41406 r140710_09 seqware r 07 / 10 / 2014 09:51:40 main.q @worker1 1",
                "   1190 0.25000 r140710_09 seqware r 07 / 10 / 2014 09:51:55 main.q @worker2 1",
                "   1189 0.00000 r140710_09 seqware hqw 07 / 10 / 2014 09:51:27 1",
                "   1191 0.00000 r140710_09 seqware hqw 07 / 10 / 2014 09:51:48 1",
                "   1192 0.00000 r140710_09 seqware hqw 07 / 10 / 2014 09:51:48 1")
    }

    @Override
    void createComputeParameter(ResourceSet resourceSet, LinkedHashMultimap<String, String> parameters) {
        parameters["-pe"] = "serial ${resourceSet.cores}"
    }

    @Override
    void createWalltimeParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters["-l"] = "h_rt=${resourceSet.walltime.toString()}"
    }

    @Override
    void createMemoryParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters["-l"] = "h_rss=${resourceSet.getMem().toString(BufferUnit.M)}"
    }
}
