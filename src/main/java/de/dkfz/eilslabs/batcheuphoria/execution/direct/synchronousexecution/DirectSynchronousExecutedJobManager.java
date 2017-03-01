/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.execution.direct.synchronousexecution;


import de.dkfz.eilslabs.batcheuphoria.config.ResourceSet;
import de.dkfz.eilslabs.batcheuphoria.execution.ExecutionService;
import de.dkfz.eilslabs.batcheuphoria.jobs.*;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class DirectSynchronousExecutedJobManager extends JobManager<DirectCommand> {


    public DirectSynchronousExecutedJobManager(ExecutionService executionService, JobManagerCreationParameters parms) {
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
    public DirectCommand createCommand(Job job, String jobName, List<ProcessingCommands> processingCommands, File tool, Map<String, String> parameters, List<String> dependencies, List<String> arraySettings) {
        return null;
    }

    @Override
    public JobDependencyID createJobDependencyID(Job job, String jobResult) {
        return new DirectCommandDependencyID(jobResult, job);
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
    public Job parseToJob(String commandString) {
        return null;
    }

    @Override
    public GenericJobInfo parseGenericJobInfo(String command) {
        return null;
    }

//    @Override
//    public Job parseToJob(ExecutionContext executionContext, String commandString) {
//        throw new RuntimeException("Not implemented yet! " + this.getClass().getName() + ".parseToJob()");
//    }
//
//    @Override
//    public GenericJobInfo parseGenericJobInfo(ExecutionContext context, String command) {
//        return null;
//    }

    @Override
    public JobResult convertToArrayResult(Job arrayChildJob, JobResult parentJobsResult, int arrayIndex) {
        throw new RuntimeException("Not implemented yet! " + this.getClass().getName() + ".convertToArrayResult()");
    }

    @Override
    public void updateJobStatus() {

    }

    @Override
    public void queryJobAbortion(List<Job> executedJobs) {

    }

    @Override
    public void addJobStatusChangeListener(Job job) {

    }

    @Override
    public String getLogFileWildcard(Job job) {
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
        return null;
    }

    @Override
    public String getSpecificJobArrayIndexIdentifier() {
        return null;
    }

    @Override
    public String getSpecificJobScratchIdentifier() {
        return null;
    }

    @Override
    public String[] peekLogFile(Job job) {
        return new String[0];
    }

    @Override
    public String getJobStateInfoLine(Job job) {

        return null;
    }

//    @Override
//    public void queryJobAbortion(List executedJobs,ExecutionService executionService) {
//        TODO something with kill
//    }

    @Override
    public Map<String, JobState> queryJobStatus(List jobIDs) {
        return new LinkedHashMap<>();
    }

    @Override
    public DirectCommand createCommand(Job job, File tool, List<String> dependencies) {
        return new DirectCommand(this, job, tool.getName(), null, job.getParameters(), null, null, dependencies, tool.getAbsolutePath(), new File("/tmp"));
    }

    @Override
    public JobResult runJob(Job job, boolean runDummy) {
        return null;
    }

//    @Override
//    public DirectCommand createCommand(Job job, ExecutionContext run, String jobName, List<ProcessingCommands> processingCommands, File tool, Map<String, String> parameters, List<String> dependencies, List<String> arraySettings) {
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
//    public DirectCommand createCommand(Job job, ExecutionContext run, String jobName, List<ProcessingCommands> processingCommands, File tool, Map<String, String> parameters, List<String> dependencies, List<String> arraySettings) {
//
//    }

    @Override
    public String getSubmissionCommand() {
        return null;
    }
}
