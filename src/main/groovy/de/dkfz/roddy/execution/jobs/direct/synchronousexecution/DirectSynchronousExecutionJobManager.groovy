/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.direct.synchronousexecution

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import groovy.transform.CompileStatic

/**
 */
@CompileStatic
class DirectSynchronousExecutionJobManager extends BatchEuphoriaJobManager<DirectCommand> {


    DirectSynchronousExecutionJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    DirectCommand createCommand(BEJob job) {
        return new DirectCommand(this, job, [])
    }

    @Override
    ExtendedJobInfo parseGenericJobInfo(String command) {
        return null
    }

    @Override
    Map<BEJobID, JobInfo> queryJobInfo(List<BEJobID> jobIDs) {
        return [:]
    }

    @Override
    Map<BEJobID, ExtendedJobInfo> queryExtendedJobInfo(List<BEJobID> jobIDs) {
        return [:]
    }

    @Override
    ExecutionResult executeKillJobs(List<BEJobID> jobIDs) {
        String command = "kill -s SIGKILL ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    @Override
    protected ExecutionResult executeStartHeldJobs(List<BEJobID> jobIDs) {
        String command = "kill -s SIGCONT ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    @Override
    void addToListOfStartedJobs(BEJob job) {}

    @Override
    boolean getDefaultForHoldJobsEnabled() {
        return false
    }

    @Override
    String getJobIdVariable() {
        return BEJob.PARM_JOBCREATIONCOUNTER
    }

    String getJobNameVariable() {
        return '$'
    }

    @Override
    String getQueueVariable() {
        return ''
    }

    @Override
    String getNodeFileVariable() {
        return null
    }

    @Override
    String getSubmitHostVariable() {
        return null
    }

    @Override
    String getSubmitDirectoryVariable() {
        return null
    }

    @Override
    Map<BEJob, JobState> queryJobStatus(List<BEJob> jobs) {
        (jobs?.collectEntries { BEJob job -> [job, JobState.UNKNOWN] } ?: [:]) as Map<BEJob, JobState>
    }

    @Override
    BEJobResult submitJob(BEJob job) {
        if (forbidFurtherJobSubmission) {
            throw new BEException("You are not allowed to submit further jobs. This happens, when you call waitForJobs().")
        }
        // Some of the parent jobs are in a bad state!
        Command command = createCommand(job)
        BEJobResult jobResult
        BEJobID jobID
        ExecutionResult res

        /** For direct execution, there might be parent jobs, which  failed or were aborted. Don't start, if this is the case.  **/
        if (job.parentJobs.findAll {
            BEJob pJob = it as BEJob
            !(pJob.getJobState() == JobState.COMPLETED_SUCCESSFUL || pJob.getJobState() == JobState.UNKNOWN)
        }
        ) {
            jobID = new BEFakeJobID(BEFakeJobID.FakeJobReason.NOT_EXECUTED)
            command.setJobID(jobID)
        } else {
            jobID = new BEJobID(job.jobCreationCounter.toString()) // Needs to be set before job.run, because console output will be wrong otherwise.
            command.job.resetJobID(jobID)
            command.setJobID(jobID)
            res = executionService.execute(command)
            if (!res.successful)
                throw new BEException("Execution of Job ${jobID} failed with exit code ${res.exitCode} and message ${res.resultLines}")
        }

        jobResult = new BEJobResult(command, job, res, job.tool, job.parameters, job.parentJobs as List<BEJob>)
        job.setRunResult(jobResult)

        return jobResult
    }

    @Override
    ProcessingParameters convertResourceSet(BEJob job, ResourceSet resourceSet) {
        return new ProcessingParameters(LinkedHashMultimap.create())
    }


    @Override
    boolean executesWithoutJobSystem() {
        return true
    }

    @Override
    String parseJobID(String commandOutput) {
        return commandOutput
    }

    @Override
    protected JobState parseJobState(String stateString) {
        return null
    }

    @Override
    String getSubmissionCommand() {
        return null
    }

    @Override
    String getQueryCommandForJobInfo() {
        return null
    }

    @Override
    String getQueryCommandForExtendedJobInfo() {
        return null
    }

    @Override
    Map<BEJobID, ExtendedJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds) {
        return [:]
    }
}
