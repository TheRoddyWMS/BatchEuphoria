/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.direct.synchronousexecution

import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.tools.LoggerWrapper

/**
 */
class DirectSynchronousExecutionJobManager extends BatchEuphoriaJobManager<DirectCommand> {

    public static final LoggerWrapper logger = LoggerWrapper.getLogger(DirectSynchronousExecutionJobManager.class.getName())

    DirectSynchronousExecutionJobManager(BEExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms)
    }

    @Override
    void createUpdateDaemonThread(int interval) {
        //Not necessary, a command / job knows its state in local execution
    }

    @Override
    DirectCommand createCommand(GenericJobInfo jobInfo) {
        return null
    }

    @Override
    DirectCommand createCommand(BEJob job, String jobName, List<ProcessingParameters> processingCommands, File tool, Map<String, String> parameters, List<String> dependencies) {
        return null
    }

    @Override
    BEJobID createJobID(BEJob job, String jobResult) {
        return new DirectCommandID(jobResult, job)
    }

    @Override
    ProcessingParameters convertResourceSet(ResourceSet resourceSet) {
        return null
    }

    @Override
    ProcessingParameters parseProcessingCommands(String pCmd) {
        return new ProcessingParameters([:])
    }

//    @Override
//    public ProcessingCommands getProcessingCommandsFromConfiguration(Configuration configuration, String toolID) {
//        return null;
//    }

    @Override
    ProcessingParameters extractProcessingCommandsFromToolScript(File file) {
        return null
    }

    @Override
    BEJob parseToJob(String commandString) {
        return null
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String command) {
        return null
    }

    @Override
    BEJobResult convertToArrayResult(BEJob arrayChildJob, BEJobResult parentJobsResult, int arrayIndex) {
        throw new RuntimeException("Not implemented yet! " + this.getClass().getName() + ".convertToArrayResult()")
    }

//    @Override
//    public BEJob parseToJob(ExecutionContext executionContext, String commandString) {
//        throw new RuntimeException("Not implemented yet! " + this.getClass().getName() + ".parseToJob()");
//    }
//
//    @Override
//    public GenericJobInfo parseGenericJobInfo(ExecutionContext context, String command) {
//        return null;
//    }

//    @Override
//    public BEJobResult convertToArrayResult(BEJob arrayChildJob, BEJobResult parentJobsResult, int arrayIndex) {
//        throw new RuntimeException("Not implemented yet! " + this.getClass().getName() + ".convertToArrayResult()");
//    }

    @Override
    void updateJobStatus() {

    }

    @Override
    void queryJobAbortion(List<BEJob> executedJobs) {

    }

    @Override
    void addJobStatusChangeListener(BEJob job) {

    }

    @Override
    String getLogFileWildcard(BEJob job) {
        return "*"
    }

    @Override
    boolean compareJobIDs(String jobID, String id) {
        return jobID.equals(id)
    }

    @Override
    String getStringForQueuedJob() {
        return null
    }

    @Override
    String getStringForJobOnHold() {
        return null
    }

    @Override
    String getStringForRunningJob() {
        return null
    }

    @Override
    String getJobIdVariable() {
        return null
    }

    @Override
    String getJobArrayIndexVariable() {
        return null
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
    String getSpecificJobIDIdentifier() {
        logger.severe("BEJob id for " + getClass().getName() + " should be configurable")
        return '"$$"'
    }

    @Override
    String getSpecificJobArrayIndexIdentifier() {
        logger.severe("BEJob arrays are not supported in " + getClass().getName())
        return "0"
    }

    @Override
    String getSpecificJobScratchIdentifier() {
        logger.severe("BEJob scratch for " + getClass().getName() + " should be configurable")
        return '/data/roddyScratch/$$'
    }

    @Override
    String[] peekLogFile(BEJob job) {
        return new String[0]
    }

    @Override
    Map<BEJob, JobState> queryJobStatus(List<BEJob> jobs) {
        jobs?.collectEntries { BEJob job -> [job, JobState.UNKNOWN] } ?: [:]
    }

    @Override
    DirectCommand createCommand(BEJob job, File tool, List<String> dependencies) {
        return new DirectCommand(this, job, tool.getName(), null, job.getParameters(), null, null, dependencies, tool.getAbsolutePath(), new File("/tmp"))
    }

    @Override
    BEJobResult runJob(BEJob job) {
        // Some of the parent jobs are in a bad state!
        Command command = createCommand(job, job.tool, [])
        BEJobResult jobResult
        BEJobID jobID
        boolean successful = false

        /** For direct execution, there might be parent jobs, which  failed or were aborted. Don't start, if this is the case.  **/
        if (job.parentJobs.findAll { BEJob pJob -> !(pJob.getJobState() == JobState.COMPLETED_SUCCESSFUL || pJob.getJobState() == JobState.UNKNOWN) }) {
            jobID = new BEFakeJobID(job, BEFakeJobID.FakeJobReason.NOT_EXECUTED)
            command.setExecutionID(jobID)
        } else {
            def res = executionService.execute(command)
            jobID = createJobID(job, parseJobID(res.processID))
            successful = res.successful
            if (!successful)
                logger.sometimes("Execution of Job ${jobID} failed with exit code ${res.exitCode} and message ${res.resultLines}")
        }

        command.setExecutionID(jobID)
        jobResult = new BEJobResult(command, jobID, successful, false, job.tool, job.parameters, job.parentJobs as List<BEJob>)
        job.setRunResult(jobResult)

        return jobResult
    }

    @Override
    ProcessingParameters convertResourceSet(BEJob job, ResourceSet resourceSet) {
        return null
    }

    @Override
    ProcessingParameters extractProcessingParametersFromToolScript(File file) {
        return null
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
    File getLoggingDirectoryForJob(BEJob job) {
        return executionService.queryWorkingDirectory()
    }

    @Override
    Map<BEJob, JobState> queryJobStatus(List<BEJob> jobs, boolean forceUpdate) {
        return null
    }

    @Override
    Map<String, JobState> queryJobStatusAll(boolean forceUpdate) {
        return null
    }

    @Override
    Map<String, JobState> queryJobStatusById(List<String> jobIds, boolean forceUpdate) {
        return null
    }

    @Override
    Map<String, BEJob> queryExtendedJobState(List<BEJob> jobs, boolean forceUpdate) {
        return null
    }

    @Override
    Map<String, GenericJobInfo> queryExtendedJobStateById(List<String> jobIds, boolean forceUpdate) {
        return null
    }

    @Override
    JobState parseJobState(String stateString) {
        return null
    }

}
