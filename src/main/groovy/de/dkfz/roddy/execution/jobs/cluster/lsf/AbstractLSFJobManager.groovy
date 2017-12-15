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
        int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
        int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1

        // The -n parameter is the amount of SLOTS!

        // If you use > 1 nodes and > 1 cores it is a bit more complicated than let's say in PBS
        // If nodes == 1, -n is the amount of cores. If nodes > 1, -n is the amount of cores multiplied by
        // the amount of nodes. In addition, you need to provide a span factor as a resource.

        parameters["-n"] = nodes == 1 ? cores : "${cores * nodes} -R \"span[ptile=${cores}]\""
    }

    @Override
    void createQueueParameter(LinkedHashMultimap<String, String> parameters, String queue) {
        parameters['-q'] = queue
    }

    @Override
    void createWalltimeParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters['-W'] = durationToLSFWallTime(resourceSet.getWalltime())
    }

    @Override
    void createMemoryParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        // LSF does not like the buffer unit at the end and always takes MB
        def memval = resourceSet.getMem().toString(BufferUnit.M)[0 .. -2]
        parameters["-M"] = "${memval} -R \"rusage[mem=${memval}]\""
    }

    @Override
    void createStorageParameters(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {

    }
}
