/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.direct.synchronousexecution

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import groovy.transform.CompileStatic

import java.time.Duration


@CompileStatic
class DirectSynchronousExecutionJobManager extends BatchEuphoriaJobManager<DirectCommand> {


    DirectSynchronousExecutionJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    @Override
    protected void createUpdateDaemonThread() {
        //Not necessary, a command / job knows its state in local execution
    }

    DirectCommand createCommand(BEJob job) {
        return new DirectCommand(this, job, [])
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String command) {
        return null
    }

    @Override
    protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs,
                                                    Duration timeout = Duration.ZERO) {
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
    Map<BEJob, JobState> queryJobStatus(List<BEJob> jobs,
                                        Duration timeout = Duration.ZERO) {
        (jobs?.collectEntries { BEJob job -> [job, JobState.UNKNOWN] } ?: [:]) as Map<BEJob, JobState>
    }

    /**
     * Submit job with the specified wallTime.
     *
     * The interface and behaviour deviates from the one of other JobManagers. The other job
     * managers are rather for asynchronous execution with a swift submission command (bsub, qsub)
     * that directly returns. The DirectSynchronousJobManager, as the name says, submits jobs
     * synchronously and the jobs's runtime usually are much longer than the execution time of
     * bsub/qsub.
     */
    @Override
    BEJobResult submitJob(BEJob job, Duration walltime = Duration.ZERO) {
        if (forbidFurtherJobSubmission) {
            throw new BEException("You are not allowed to submit further jobs. This happens, when you call waitForJobs().")
        }
        // Some of the parent jobs are in a bad state!
        Command command = createCommand(job)
        BEJobResult jobResult
        BEJobID jobID
        ExecutionResult res

        /** For direct execution, there may be parent jobs, which failed or were aborted.
         *  Don't start, if this is the case.  **/
        if (job.parentJobs.findAll {
            BEJob pJob = it as BEJob
            !(pJob.jobState == JobState.COMPLETED_SUCCESSFUL || pJob.jobState == JobState.UNKNOWN)
        }) {
            jobID = new BEFakeJobID(BEFakeJobID.FakeJobReason.NOT_EXECUTED)
            command.setJobID(jobID)
        } else {
            jobID = new BEJobID(job.jobCreationCounter.toString()) // Needs to be set before job.run, because console output will be wrong otherwise.
            command.job.resetJobID(jobID)
            command.setJobID(jobID)
            res = executionService.execute(command, walltime)
            if (!res.successful)
                throw new BEException("Execution of Job ${jobID} failed: ${res.toStatusLine()}")
        }

        jobResult = new BEJobResult(command, job, res, job.parameters, job.parentJobs as List<BEJob>)
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
    String getQueryJobStatesCommand() {
        return null
    }

    @Override
    String getExtendedQueryJobStatesCommand() {
        return null
    }

    @Override
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds,
                                                           Duration timeout = Duration.ZERO) {
        return [:]
    }
}
