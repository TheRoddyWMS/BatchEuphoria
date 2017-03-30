/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria

import de.dkfz.eilslabs.batcheuphoria.execution.ExecutionService
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
        def er = execute("${command}", waitFor)
        if (er.successful) {
            // Find the one entry in a range of line which starts with a number.
            // In my case, several additional error lines with Bash error messages appeared in changing order and
            // this solution guaranteed that I always get the right line.
            command.job.runResult = new JobResult(command, new PBSJobDependencyID(command.job, er.resultLines.find { it.split("[.]")[0].isNumber() }), true, command.job.tool, command.job.parameters, command.job.parentJobs as List)
        } else {
            command.job.runResult = new JobResult(command, new PBSJobDependencyID(command.job, "-1"), false, command.job.tool, command.job.parameters, command.job.parentJobs as List)
        }
        return er
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
    boolean isAvailable() {
        return true
    }
}
