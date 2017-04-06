/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria

import de.dkfz.eilslabs.batcheuphoria.execution.ExecutionService
import de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest.RestCommand
import de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest.RestResult
import de.dkfz.eilslabs.batcheuphoria.execution.cluster.pbs.PBSJobDependencyID
import de.dkfz.eilslabs.batcheuphoria.jobs.Command
import de.dkfz.roddy.execution.io.ExecutionHelper
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.JobResult
import groovy.transform.CompileStatic

/**
 * Created by heinold on 27.03.17.
 */
@CompileStatic
class TestExecutionService implements ExecutionService {

    String user
    String server

    TestExecutionService(String user, String server) {
        this.user = user
        this.server = server
    }

    @Override
    ExecutionResult execute(Command command, boolean waitFor = true) {
        return execute("${command}", waitFor)
    }

    @Override
    ExecutionResult execute(String command, boolean waitFor = true) {
        return ExecutionHelper.executeCommandWithExtendedResult("ssh ${user}@${server} ${command}")
    }

    ExecutionResult executeLocal(String command) {
        return ExecutionHelper.executeCommandWithExtendedResult(command)
    }

    void copyFileToRemote(File file, File remote) {
        String cmd = "scp ${file} ${user}@${server}:${remote}"
        ExecutionResult er = executeLocal(cmd)
        if (!er.successful)
            println("$cmd returned an error.")
    }

    @Override
    ExecutionResult execute(String command, boolean waitForIncompatibleClassChangeError, OutputStream outputStream) {
        return null
    }

    @Override
    RestResult execute(RestCommand restCommand) {
        return null
    }

    @Override
    boolean isAvailable() {
        return true
    }

    @Override
    String handleServiceBasedJobExitStatus(Command command, ExecutionResult res, OutputStream outputStream) {
        return null
    }
}
