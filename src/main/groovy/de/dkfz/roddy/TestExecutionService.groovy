/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy

import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.LocalExecutionHelper
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.tools.BashUtils
import groovy.transform.CompileStatic

import java.time.Duration

/**
 * Created by heinold on 27.03.17.
 */
@CompileStatic
class TestExecutionService implements BEExecutionService {

    String user
    String server

    TestExecutionService(String user, String server) {
        this.user = user
        this.server = server
    }

    @Override
    ExecutionResult execute(Command command, boolean waitFor = true,
                            Duration timeout = Duration.ZERO) {
        return execute("${command}", waitFor, timeout)
    }

    @Override
    ExecutionResult execute(String command, boolean waitFor = true,
                            Duration timeout = Duration.ZERO) {
        String sshCommand = "ssh ${user}@${server} ${BashUtils.strongQuote(command)}"
        return LocalExecutionHelper.executeCommandWithExtendedResult(sshCommand)
    }

    ExecutionResult executeLocal(String command, Duration timeout = Duration.ZERO) {
        return LocalExecutionHelper.executeCommandWithExtendedResult(command)
    }

    void copyFileToRemote(File file, File remote) {
        String cmd = "scp ${file} ${user}@${server}:${remote}"
        ExecutionResult er = executeLocal(cmd)
        if (!er.successful)
            println("$cmd returned an error.")
    }

    @Override
    boolean isAvailable() {
        return true
    }

    @Override
    File queryWorkingDirectory() {
        return null
    }

    @Override
    ExecutionResult execute(Command command, Duration timeout) {
        return execute(command, true, timeout)
    }

    @Override
    ExecutionResult execute(String command, Duration timeout) {
       return execute(command, true, timeout)
    }
}
