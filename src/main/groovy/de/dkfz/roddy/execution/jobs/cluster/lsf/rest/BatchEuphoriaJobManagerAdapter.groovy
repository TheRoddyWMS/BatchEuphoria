/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf.rest

import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.cluster.ClusterJobManager
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.JobManagerCreationParameters
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.execution.jobs.ProcessingCommands
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.BEJobResult
import sun.reflect.generics.reflectiveObjects.NotImplementedException

/**
 * Created by kaercher on 22.03.17.
 */
class BatchEuphoriaJobManagerAdapter extends ClusterJobManager {
    BatchEuphoriaJobManagerAdapter(BEExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms)
    }

    @Override
    Command createCommand(GenericJobInfo jobInfo) {
        throw new NotImplementedException()
    }

    @Override
    BEJobResult runJob(BEJob job) {
        throw new NotImplementedException()
    }

    @Override
    BEJobID createJobID(BEJob job, String jobResult) {
        throw new NotImplementedException()
    }

    // needed
    @Override
    ProcessingCommands convertResourceSet(ResourceSet resourceSet) {
        throw new NotImplementedException()
    }

    @Override
    ProcessingCommands parseProcessingCommands(String alignmentProcessingOptions) {
        throw new NotImplementedException()
    }

    @Override
    ProcessingCommands extractProcessingCommandsFromToolScript(File file) {
        throw new NotImplementedException()
    }

    @Override
    BEJob parseToJob(String commandString) {
        throw new NotImplementedException()
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String command) {
        throw new NotImplementedException()
    }

    @Override
    BEJobResult convertToArrayResult(BEJob arrayChildJob, BEJobResult parentJobsResult, int arrayIndex) {
        throw new NotImplementedException()
    }
    // needed
    @Override
    void updateJobStatus() {
        throw new NotImplementedException()
    }

    @Override
    Map<BEJob, GenericJobInfo> queryExtendedJobState(List list, boolean forceUpdate) {
        throw new NotImplementedException()
    }

    @Override
    Map<String, GenericJobInfo> queryExtendedJobStateById(List list, boolean forceUpdate) {
        throw new NotImplementedException()
    }

    @Override
    void addJobStatusChangeListener(BEJob job) {
        throw new NotImplementedException()
    }

    @Override
    String getLogFileWildcard(BEJob job) {
        throw new NotImplementedException()
    }

    // needed
    @Override
    boolean compareJobIDs(String jobID, String id) {
        return false
    }

    @Override
    String getStringForQueuedJob() {
        throw new NotImplementedException()
    }

    @Override
    String getStringForJobOnHold() {
        throw new NotImplementedException()
    }

    @Override
    String getStringForRunningJob() {
        throw new NotImplementedException()
    }

    @Override
    String getSpecificJobIDIdentifier() {
        throw new NotImplementedException()
    }

    @Override
    String getSpecificJobArrayIndexIdentifier() {
        throw new NotImplementedException()
    }

    @Override
    String[] peekLogFile(BEJob job) {
        return new String[0]
    }

    @Override
    String parseJobID(String commandOutput) {
        throw new NotImplementedException()
    }

    @Override
    String getSubmissionCommand() {
        throw new NotImplementedException()
    }

    @Override
    File getLoggingDirectoryForJob(BEJob job) {
        throw new NotImplementedException()
    }
// needed
    @Override
    void queryJobAbortion(List executedJobs) {
        throw new NotImplementedException()
    }

    // needed
    @Override
    Map<String, JobState> queryJobStatus(List jobIDs) {
        throw new NotImplementedException()
    }

    @Override
    Command createCommand(BEJob job, String jobName, List processingCommands, File tool, Map parameters, List dependencies) {
        throw new NotImplementedException()
    }

    @Override
    Map<BEJob, JobState> queryJobStatus(List list, boolean forceUpdate) {
        throw new NotImplementedException()
    }

    @Override
    Map<String, JobState> queryJobStatusById(List<String> jobIds, boolean forceUpdate) {
        throw new NotImplementedException()
    }

    @Override
    Map<String, JobState> queryJobStatusAll(boolean forceUpdate) {
        throw new NotImplementedException()
    }

    @Override
    JobState parseJobState(String stateString) {
        throw new NotImplementedException()
    }
}
