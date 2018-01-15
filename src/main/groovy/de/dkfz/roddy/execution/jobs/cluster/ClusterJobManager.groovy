/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.tools.LoggerWrapper
import groovy.transform.CompileStatic

import java.time.Duration

/**
 * A class for processing backends running on a cluster.
 * This mainly defines variables and constants which can be set via the config.
 */
@CompileStatic
abstract class ClusterJobManager<C extends Command> extends BatchEuphoriaJobManager<C> {
    private static final LoggerWrapper logger = LoggerWrapper.getLogger(BatchEuphoriaJobManager.class.getSimpleName());

    ClusterJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    protected static <T> T catchExceptionAndLog(final Closure<T> closure) {
        try {
            return closure.call()
        } catch (Exception e) {
            logger.warning(e.message)
            logger.warning(e.stackTrace.join("\n"))
        }
        return null
    }

    protected static Duration parseColonSeparatedHHMMSSDuration(String str) {
        String[] hhmmss = str.split(":")
        if (hhmmss.size() != 3) {
            throw new BEException("Duration string is not of the format HH+:MM:SS: '${str}'")
        }
        return Duration.parse(String.format("PT%sH%sM%sS", hhmmss))
    }

    @Override
    ProcessingParameters convertResourceSet(BEJob job, ResourceSet resourceSet) {
        assert resourceSet

        LinkedHashMultimap<String, String> parameters = LinkedHashMultimap.create()

        createDefaultManagerParameters(parameters)

        if (requestMemoryIsEnabled && resourceSet.isMemSet())
            createMemoryParameter(parameters, resourceSet)

        if (requestWalltimeIsEnabled && resourceSet.isWalltimeSet())
            createWalltimeParameter(parameters, resourceSet)

        if (requestQueueIsEnabled && resourceSet.isQueueSet())
            createQueueParameter(parameters, resourceSet.getQueue())

        if (requestQueueIsEnabled && job?.customQueue)
            createQueueParameter(parameters, job.customQueue)

        if (requestCoresIsEnabled && resourceSet.isCoresSet() || resourceSet.isNodesSet())
            createComputeParameter(resourceSet, parameters)

        if (requestStorageIsEnabled && resourceSet.isStorageSet())
            createStorageParameters(parameters, resourceSet)

        return new ProcessingParameters(parameters)
    }

    abstract void createDefaultManagerParameters(LinkedHashMultimap<String, String> parameters)

    abstract void createComputeParameter(ResourceSet resourceSet, LinkedHashMultimap<String, String> parameters)
    abstract void createQueueParameter(LinkedHashMultimap<String, String> parameters, String queue)
    abstract void createWalltimeParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet)
    abstract void createMemoryParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet)
    abstract void createStorageParameters(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet)
}
