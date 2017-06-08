/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution

import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.io.ExecutionResult
import groovy.transform.CompileStatic

/**
 * Created by kaercher on 12.01.17.
 */
@CompileStatic
interface BEExecutionService {

    ExecutionResult execute(Command command)

    ExecutionResult execute(Command command, boolean waitFor)

    ExecutionResult execute(String command)

    ExecutionResult execute(String command, boolean waitFor)

    ExecutionResult execute(String command, boolean waitForIncompatibleClassChangeError, OutputStream outputStream)


    /**
     * Query this to find out, if the service is still active
      */
    boolean isAvailable()

    String handleServiceBasedJobExitStatus(Command command, ExecutionResult res, OutputStream outputStream)

    File queryWorkingDirectory()
}