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

    public static final LoggerWrapper logger = LoggerWrapper.getLogger(DirectSynchronousExecutionJobManager.class.getName());

    public DirectSynchronousExecutionJobManager(BEExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms);
    }

    @Override
    public void createUpdateDaemonThread(int interval) {
        //Not necessary, a command / job knows its state in local execution
    }

    @Override
    public DirectCommand createCommand(GenericJobInfo jobInfo) {
        return null;
    }

    @Override
    public DirectCommand createCommand(BEJob job, String jobName, List<ProcessingCommands> processingCommands, File tool, Map<String, String> parameters, List<String> dependencies) {
        return null;
    }

    @Override
    public BEJobID createJobID(BEJob job, String jobResult) {
        return new DirectCommandID(jobResult, job);
    }

    @Override
    public ProcessingCommands convertResourceSet(ResourceSet resourceSet) {
        return null;
    }

    @Override
    public ProcessingCommands parseProcessingCommands(String pCmd) {
        return new DummyProcessingCommand(pCmd);
    }

//    @Override
//    public ProcessingCommands getProcessingCommandsFromConfiguration(Configuration configuration, String toolID) {
//        return null;
//    }

    @Override
    public ProcessingCommands extractProcessingCommandsFromToolScript(File file) {
        return null;
    }

    @Override
    public BEJob parseToJob(String commandString) {
        return null;
    }

    @Override
    public GenericJobInfo parseGenericJobInfo(String command) {
        return null;
    }

    @Override
    public BEJobResult convertToArrayResult(BEJob arrayChildJob, BEJobResult parentJobsResult, int arrayIndex) {
        throw new RuntimeException("Not implemented yet! " + this.getClass().getName() + ".convertToArrayResult()");
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
    public void updateJobStatus() {

    }

    @Override
    public void queryJobAbortion(List<BEJob> executedJobs) {

    }

    @Override
    public void addJobStatusChangeListener(BEJob job) {

    }

    @Override
    public String getLogFileWildcard(BEJob job) {
        return "*";
    }

    @Override
    public boolean compareJobIDs(String jobID, String id) {
        return jobID.equals(id);
    }

    @Override
    public String getStringForQueuedJob() {
        return null;
    }

    @Override
    public String getStringForJobOnHold() {
        return null;
    }

    @Override
    public String getStringForRunningJob() {
        return null;
    }

    @Override
    public String getSpecificJobIDIdentifier() {
        logger.severe("BEJob id for " + getClass().getName() + " should be configurable");
        return '"$$"'
    }

    @Override
    public String getSpecificJobArrayIndexIdentifier() {
        logger.severe("BEJob arrays are not supported in " + getClass().getName());
        return "0";
    }

    @Override
    public String getSpecificJobScratchIdentifier() {
        logger.severe("BEJob scratch for " + getClass().getName() + " should be configurable");
        return '/data/roddyScratch/$$'
    }

    @Override
    public String[] peekLogFile(BEJob job) {
        return new String[0];
    }

//    @Override
//    public void queryJobAbortion(List executedJobs,BEExecutionService executionService) {
//        TODO something with kill
//    }

    @Override
    public Map<BEJob, JobState> queryJobStatus(List<BEJob> jobs) {
        jobs?.collectEntries { BEJob job -> [job, JobState.UNKNOWN] } ?: []
    }

    @Override
    public DirectCommand createCommand(BEJob job, File tool, List<String> dependencies) {
        return new DirectCommand(this, job, tool.getName(), null, job.getParameters(), null, null, dependencies, tool.getAbsolutePath(), new File("/tmp"));
    }

    @Override
    public BEJobResult runJob(BEJob job) {
        return null;
    }

//    @Override
//    public DirectCommand createCommand(BEJob job, ExecutionContext run, String jobName, List<ProcessingCommands> processingCommands, File tool, Map<String, String> parameters, List<String> dependencies, List<String> arraySettings) {
//        return new DirectCommand(job, run, jobName, processingCommands, parameters, dependencies, arraySettings, tool.getAbsolutePath());
//    }

    @Override
    public boolean executesWithoutJobSystem() {
        return true;
    }

    @Override
    public String parseJobID(String commandOutput) {
        return commandOutput;
    }
    //    @Override
//    public DirectCommand createCommand(BEJob job, ExecutionContext run, String jobName, List<ProcessingCommands> processingCommands, File tool, Map<String, String> parameters, List<String> dependencies, List<String> arraySettings) {
//
//    }

    @Override
    public String getSubmissionCommand() {
        return null;
    }

    @Override
    File getLoggingDirectoryForJob(BEJob job) {
        return executionService.queryWorkingDirectory()
    }

    @Override
    public Map<BEJob, JobState> queryJobStatus(List<BEJob> jobs, boolean forceUpdate) {
        return null;
    }

    @Override
    public Map<String, JobState> queryJobStatusAll(boolean forceUpdate) {
        return null;
    }

    @Override
    public Map<String, JobState> queryJobStatusById(List<String> jobIds, boolean forceUpdate) {
        return null;
    }

    @Override
    public Map<String, BEJob> queryExtendedJobState(List<BEJob> jobs, boolean forceUpdate) {
        return null;
    }

    @Override
    public Map<String, GenericJobInfo> queryExtendedJobStateById(List<String> jobIds, boolean forceUpdate) {
        return null;
    }

    @Override
    JobState parseJobState(String stateString) {
        return null
    }

}
