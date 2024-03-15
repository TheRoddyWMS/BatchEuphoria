/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution

import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.Command
import groovy.transform.CompileStatic

import java.time.Duration

/**
 * TODO Making this an abstract method would allow to set default values or add default
 *      implementations that forward requests to other execute methods.
 */
@CompileStatic
interface BEExecutionService {

    ExecutionResult execute(Command command)

    ExecutionResult execute(de.dkfz.roddy.execution.jobs.Command command, boolean waitFor)

    ExecutionResult execute(Command command, Duration timeout)

    ExecutionResult execute(Command command, boolean waitFor, Duration timeout)


    @Deprecated
    ExecutionResult execute(String command)

    @Deprecated
    ExecutionResult execute(String command, boolean waitFor)

    @Deprecated
    ExecutionResult execute(String command, Duration timeout)

    @Deprecated
    ExecutionResult execute(String command, boolean waitFor, Duration timeout)

    /**
     * Query this to find out, if the service is still active
      */
    boolean isAvailable()

    File queryWorkingDirectory()
}
