/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.execution

import de.dkfz.eilslabs.batcheuphoria.jobs.Command
import de.dkfz.roddy.execution.io.ExecutionResult
import groovy.transform.CompileStatic

/**
 * Created by kaercher on 12.01.17.
 */
@CompileStatic
interface ExecutionService {

    ExecutionResult execute(Command command)

    ExecutionResult execute(Command command, boolean waitFor)

    ExecutionResult execute(String command)

    ExecutionResult execute(String command, boolean waitFor)

    /**
     * Query this to find out, if the service is still active
      */
    boolean isAvailable()
}