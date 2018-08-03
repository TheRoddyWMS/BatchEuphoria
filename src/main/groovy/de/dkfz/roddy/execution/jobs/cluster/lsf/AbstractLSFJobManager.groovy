/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.ClusterJobManager
import de.dkfz.roddy.tools.BufferUnit
import groovy.transform.CompileStatic

import java.time.Duration

/**
 * Created by kaercher on 22.03.17.
 */
@CompileStatic
abstract class AbstractLSFJobManager extends ClusterJobManager<LSFCommand> {

    AbstractLSFJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    @Override
    String getJobIdVariable() {
        return "LSB_JOBID"
    }

    @Override
    String getJobNameVariable() {
        return "LSB_JOBNAME"
    }

    @Override
    String getQueueVariable() {
        return 'LSB_QUEUE'
    }

    @Override
    String getNodeFileVariable() {
        return "LSB_HOSTS"
    }

    @Override
    String getSubmitHostVariable() {
        return "LSB_SUB_HOST"
    }

    @Override
    String getSubmitDirectoryVariable() {
        return "LSB_SUBCWD"
    }

    @Override
    List<String> getEnvironmentVariableGlobs() {
        return Collections.unmodifiableList(["LSB_*", "LS_*"])
    }

    private String durationToLSFWallTime(Duration wallTime) {
        if (wallTime) {
            return String.valueOf(wallTime.toMinutes())
        }
        return null
    }

    @Override
    void createDefaultManagerParameters(LinkedHashMultimap<String, String> parameters) {

    }

    @Override
    void createComputeParameter(ResourceSet resourceSet, LinkedHashMultimap<String, String> parameters) {
        int nodes = resourceSet.isNodesSet() && resourceSet.getNodes() > 0 ? resourceSet.getNodes() : 1
        int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1

        // The -n parameter is the amount of SLOTS!

        // If you use > 1 nodes and > 1 cores it is a bit more complicated than let's say in PBS
        // If nodes == 1, -n is the amount of cores. If nodes > 1, -n is the amount of cores multiplied by
        // the amount of nodes. In addition, you need to provide a span factor as a resource.
        // Unfortunately, we had some errors when span was not set. It happened, that the job was spread over
        // several nodes. To prevent this, we can add the hosts=1 span attribute. BUT: This only works for a
        // span of 1, not for more. To have more hosts, you need ptile.

        def s = ""
        if (nodes == 1) s = "${cores} -R \"span[hosts=1]\""
        else s = "${cores * nodes} -R \"span[ptile=${cores}]\""
        parameters.put("-n", s)
    }

    @Override
    void createQueueParameter(LinkedHashMultimap<String, String> parameters, String queue) {
        parameters.put('-q', queue)
    }

    @Override
    void createWalltimeParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put('-W', durationToLSFWallTime(resourceSet.getWalltime()))
    }

    @Override
    void createMemoryParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        // LSF does not like the buffer unit at the end and always takes MB
        def memval = resourceSet.getMem().toResourceStringWithoutUnit(BufferUnit.M)
        parameters.put("-M", "${memval} -R \"rusage[mem=${memval}]\"")
    }

    @Override
    void createStorageParameters(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {

    }
}
