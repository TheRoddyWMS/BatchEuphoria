/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster

import com.google.common.base.Preconditions
import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.AnyEscapableString
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Duration

/**
 * A class for processing backends running on a cluster.
 * This mainly defines variables and constants which can be set via the config.
 */
@CompileStatic
abstract class ClusterJobManager<C extends Command> extends BatchEuphoriaJobManager<C> {
    final static Logger log = LoggerFactory.getLogger(ClusterJobManager.class)

    ClusterJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    protected static <T> T withCaughtAndLoggedException(final Closure<T> closure) {
        try {
            return closure.call()
        } catch (Exception e) {
            log.warn(e.message)
            List<StackTraceElement> stel = []
            for (StackTraceElement element : e.stackTrace) {
                stel.add(element)
                if (element.toString().contains("JobManager")) {
                    break
                }
            }
            log.warn(stel.join("\n"))
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
    boolean executesWithoutJobSystem() {
        return true
    }

    @Override
    ProcessingParameters convertResourceSet(BEJob job, ResourceSet resourceSet) {
        Preconditions.checkArgument(resourceSet != null)

        LinkedHashMultimap<String, AnyEscapableString> parameters = LinkedHashMultimap.create()

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

    abstract void createDefaultManagerParameters(LinkedHashMultimap<String, AnyEscapableString> parameters)

    abstract void createComputeParameter(ResourceSet resourceSet, LinkedHashMultimap<String, AnyEscapableString> parameters)

    abstract void createQueueParameter(LinkedHashMultimap<String, AnyEscapableString> parameters, String queue)

    abstract void createWalltimeParameter(LinkedHashMultimap<String, AnyEscapableString> parameters, ResourceSet resourceSet)

    abstract void createMemoryParameter(LinkedHashMultimap<String, AnyEscapableString> parameters, ResourceSet resourceSet)

    abstract void createStorageParameters(LinkedHashMultimap<String, AnyEscapableString> parameters, ResourceSet resourceSet)
}
