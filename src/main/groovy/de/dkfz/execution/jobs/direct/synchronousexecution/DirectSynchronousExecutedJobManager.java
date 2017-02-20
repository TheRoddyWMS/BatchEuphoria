/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.execution.jobs.direct.synchronousexecution;

import de.dkfz.config.AppConfig;
import de.dkfz.config.ExecutionService;
import de.dkfz.config.ResourceSet;
import de.dkfz.execution.jobs.JobManager;
import de.dkfz.execution.jobs.*;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class DirectSynchronousExecutedJobManager extends JobManager<DirectCommand> {


    @Override
    public void createUpdateDaemonThread(int interval) {
        //Not necessary, a command / job knows its state in local de.dkfz.execution
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
    public ProcessingCommands convertResourceSet(AppConfig configuration, ResourceSet resourceSet) {
        return null;
    }

    @Override
    public ProcessingCommands parseProcessingCommands(String pCmd) {
        return new DummyProcessingCommand(pCmd);
    }

    @Override
    public ProcessingCommands getProcessingCommandsFromConfiguration(AppConfig configuration, String toolID) {
        return null;
    }

    @Override
    public ProcessingCommands extractProcessingCommandsFromToolScript(File file) {
        return null;
    }

    @Override
    public Job parseToJob(String commandString) {
        throw new RuntimeException("Not implemented yet! " + this.getClass().getName() + ".parseToJob()");
    }

    @Override
    public GenericJobInfo parseGenericJobInfo(String command) {
        return null;
    }

    @Override
    public JobResult convertToArrayResult(Job arrayChildJob, JobResult parentJobsResult, int arrayIndex) {
        throw new RuntimeException("Not implemented yet! " + this.getClass().getName() + ".convertToArrayResult()");
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
    public String[] peekLogFile(Job job) {
        return new String[0];
    }

    @Override
    public void storeJobStateInfo(Job job) {

    }

    @Override
    public void queryJobAbortion(List executedJobs,ExecutionService executionService) {
        //TODO something with kill
    }

    @Override
    public Map<String, JobState> queryJobStatus(List jobIDs) {
        return new LinkedHashMap<>();
    }


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
