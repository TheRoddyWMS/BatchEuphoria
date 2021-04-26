/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution

import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.Command
import groovy.transform.CompileStatic

@CompileStatic
interface BEExecutionService {

    ExecutionResult execute(Command command)

    ExecutionResult execute(Command command, boolean waitFor)

    ExecutionResult execute(String command)

    ExecutionResult execute(String command, boolean waitFor)

    /**
     * Query this to find out, if the service is still active
      */
    boolean isAvailable()

    File queryWorkingDirectory()
}
