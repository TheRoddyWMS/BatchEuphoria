/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster

import de.dkfz.roddy.BEException
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSCommand
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

    public static final String CVALUE_ENFORCE_SUBMISSION_TO_NODES="enforceSubmissionToNodes";

    ClusterJobManager(BEExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms)
    }

    protected static <T> T catchExceptionAndLog(final Closure<T> closure) {
        try {
            return closure.call()
        } catch (Exception e) {
            println "*********************** conversion fail"
            println e.message
            println (e.stackTrace.join("\n"))
            println "*********************** ***************"

            logger.warning(e.message)
            logger.warning(e.stackTrace.join("\n"))
        }
        return null
    }

    static Duration parseColonSeparatedHHMMSSDuration(String str) {
        String[] hhmmss = str.split(":")
        if (hhmmss.size() != 3) {
            throw new BEException("Duration string is not of the format HH+:MM:SS: '${str}'")
        }
        return Duration.parse(String.format("PT%sH%sM%sS", hhmmss))
    }

}
