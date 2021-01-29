/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.cluster.ClusterJobManager
import de.dkfz.roddy.tools.BufferUnit
import groovy.transform.CompileStatic

import java.time.Duration

@CompileStatic
abstract class AbstractLSFJobManager extends ClusterJobManager<LSFSubmissionCommand> {

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

    /**
     * LSF supports retrying the submission command multiple times. The default is to retry for a very long time,
     * which is also blocking the execution of the thread. A single retry usually works but is failing
     * too frequently, in particular if there is load on the LSF system. The current number of LSB_NTRIES is a
     * compromise between blocking endlessly and having no failover.
     *
     * @return a Bash environment variable declaration affecting LSF commands.
     */
    final static String getEnvironmentString() {
        return "LSB_NTRIES=5"
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
