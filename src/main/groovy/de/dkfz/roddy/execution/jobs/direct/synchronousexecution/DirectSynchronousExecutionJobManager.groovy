/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.direct.synchronousexecution

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.tools.LoggerWrapper
import groovy.transform.CompileStatic

/**
 */
@CompileStatic
class DirectSynchronousExecutionJobManager extends BatchEuphoriaJobManager<DirectCommand> {

    public static final LoggerWrapper logger = LoggerWrapper.getLogger(DirectSynchronousExecutionJobManager.class.getName())

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
    protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs) {
        return [:]
    }

    @Override
    void killJobs(List<BEJob> executedJobs) {

    }

    @Override
    void addToListOfStartedJobs(BEJob job) {}

    @Override
    String getJobIdVariable() {
        return ""
    }

    String getSpecificJobIDIdentifier() {
        logger.severe("BEJob jobName for " + getClass().getName() + " should be configurable")
        return '"$$"'
    }

    String getSpecificJobScratchIdentifier() {
        logger.severe("BEJob scratch for " + getClass().getName() + " should be configurable")
        return '/data/roddyScratch/$$'
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
        // Some of the parent jobs are in a bad state!
        Command command = createCommand(job)
        BEJobResult jobResult
        BEJobID jobID
        ExecutionResult res
        boolean successful = false

        /** For direct execution, there might be parent jobs, which  failed or were aborted. Don't start, if this is the case.  **/
        if (job.parentJobs.findAll {
            BEJob pJob = it as BEJob
            !(pJob.getJobState() == JobState.COMPLETED_SUCCESSFUL || pJob.getJobState() == JobState.UNKNOWN)
        }
        ) {
            jobID = new BEFakeJobID(BEFakeJobID.FakeJobReason.NOT_EXECUTED)
            command.setJobID(jobID)
        } else {
            res = executionService.execute(command)
            jobID = new BEJobID(parseJobID(res.processID))
            successful = res.successful
            if (!successful)
                logger.sometimes("Execution of Job ${jobID} failed with exit code ${res.exitCode} and message ${res.resultLines}")
        }

        command.setJobID(jobID)
        jobResult = new BEJobResult(command, job, res, job.tool, job.parameters, job.parentJobs as List<BEJob>)
        job.setRunResult(jobResult)

        return jobResult
    }

    @Override
    void startHeldJobs(List<BEJob> jobs) { }

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
    String getSubmissionCommand() {
        return null
    }

    @Override
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds) {
        return [:]
    }
}
