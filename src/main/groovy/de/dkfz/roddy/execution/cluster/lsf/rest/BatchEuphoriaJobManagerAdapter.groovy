/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.cluster.lsf.rest

import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.cluster.ClusterJobManager
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.JobManagerCreationParameters
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.execution.jobs.ProcessingCommands
import de.dkfz.roddy.execution.jobs.JobDependencyID
import de.dkfz.roddy.execution.jobs.JobResult
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
    JobResult runJob(BEJob job) {
        return null
    }

    @Override
    JobDependencyID createJobDependencyID(BEJob job, String jobResult) {
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
    JobResult convertToArrayResult(BEJob arrayChildJob, JobResult parentJobsResult, int arrayIndex) {
        throw new NotImplementedException()
    }
    // needed
    @Override
    void updateJobStatus() {

    }

    @Override
    Map<BEJob, GenericJobInfo> queryExtendedJobState(List list, boolean forceUpdate) {
        return null
    }

    @Override
    Map<String, GenericJobInfo> queryExtendedJobStateById(List list, boolean forceUpdate) {
        return null
    }

    @Override
    void addJobStatusChangeListener(BEJob job) {

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
    String getSpecificJobScratchIdentifier() {
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

    }

    // needed
    @Override
    Map<String, JobState> queryJobStatus(List jobIDs) {
        throw new NotImplementedException()
    }

    @Override
    Command createCommand(BEJob job, String jobName, List processingCommands, File tool, Map parameters, List dependencies, List arraySettings) {
        throw new NotImplementedException()
    }

    @Override
    Map<BEJob, JobState> queryJobStatus(List list, boolean forceUpdate) {
        return null
    }

    @Override
    Map<String, JobState> queryJobStatusById(List jobIds, boolean forceUpdate) {
        return null
    }

    @Override
    Map<String, JobState> queryJobStatusAll(boolean forceUpdate) {
        return null
    }

    @Override
    protected JobState parseJobState(String stateString) {
        return null
    }
}
