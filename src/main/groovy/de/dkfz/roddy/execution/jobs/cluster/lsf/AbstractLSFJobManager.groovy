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
import de.dkfz.roddy.execution.jobs.cluster.lsf.LSFCommand
import de.dkfz.roddy.tools.BufferUnit
import groovy.transform.CompileStatic
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import java.time.Duration

/**
 * Created by kaercher on 22.03.17.
 */
@CompileStatic
abstract class AbstractLSFJobManager extends ClusterJobManager<LSFCommand> {

    AbstractLSFJobManager(BEExecutionService executionService, JobManagerCreationParameters parms) {
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
    ProcessingParameters convertResourceSet(BEJob job, ResourceSet resourceSet) {
        LinkedHashMultimap<String, String> resourceParameters = LinkedHashMultimap.create()
        if (resourceSet.isQueueSet()) {
            resourceParameters.put('-q', resourceSet.getQueue())
        }
        if (resourceSet.isMemSet()) {
            String memo = resourceSet.getMem().toString(BufferUnit.M)
            resourceParameters.put('-M', memo.substring(0, memo.toString().length() - 1))
        }
        if (resourceSet.isWalltimeSet()) {
            resourceParameters.put('-W', durationToLSFWallTime(resourceSet.getWalltimeAsDuration()))
        }
        if (resourceSet.isCoresSet() || resourceSet.isNodesSet()) {
            int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
            resourceParameters.put('-n', nodes.toString())
        }
        return new ProcessingParameters(resourceParameters)
    }

    private String durationToLSFWallTime(Duration wallTime) {
        if (wallTime) {
            return String.valueOf(wallTime.toMinutes())
        }
        return null
    }
}
