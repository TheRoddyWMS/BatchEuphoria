package de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest

import de.dkfz.eilslabs.batcheuphoria.config.ResourceSet
import de.dkfz.eilslabs.batcheuphoria.execution.ExecutionService
import de.dkfz.eilslabs.batcheuphoria.execution.cluster.ClusterJobManager
import de.dkfz.eilslabs.batcheuphoria.jobs.Command
import de.dkfz.eilslabs.batcheuphoria.jobs.GenericJobInfo
import de.dkfz.eilslabs.batcheuphoria.jobs.Job
import de.dkfz.eilslabs.batcheuphoria.jobs.JobManagerCreationParameters
import de.dkfz.eilslabs.batcheuphoria.jobs.JobState
import de.dkfz.eilslabs.batcheuphoria.jobs.ProcessingCommands
import de.dkfz.roddy.execution.jobs.JobDependencyID
import de.dkfz.roddy.execution.jobs.JobResult
import sun.reflect.generics.reflectiveObjects.NotImplementedException

/**
 * Created by kaercher on 22.03.17.
 */
class JobManagerAdapter extends ClusterJobManager{
    JobManagerAdapter(ExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms)
    }

    @Override
    Command createCommand(GenericJobInfo jobInfo) {
        throw new NotImplementedException()
    }

    // needed
    @Override
    JobResult runJob(Job job, boolean runDummy) {
        throw new NotImplementedException()
    }

    @Override
    JobDependencyID createJobDependencyID(Job job, String jobResult) {
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
    Job parseToJob(String commandString) {
        throw new NotImplementedException()
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String command) {
        throw new NotImplementedException()
    }

    @Override
    JobResult convertToArrayResult(Job arrayChildJob, JobResult parentJobsResult, int arrayIndex) {
        throw new NotImplementedException()
    }
    // needed
    @Override
    void updateJobStatus() {

    }

    @Override
    Map<Job, GenericJobInfo> queryExtendedJobState(List list, boolean forceUpdate) {
        return null
    }

    @Override
    void addJobStatusChangeListener(Job job) {

    }

    @Override
    String getLogFileWildcard(Job job) {
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
    String[] peekLogFile(Job job) {
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
    Command createCommand(Job job, String jobName, List processingCommands, File tool, Map parameters, List dependencies, List arraySettings) {
        throw new NotImplementedException()
    }

    @Override
    Map<Job, JobState> queryJobStatus(List list, boolean forceUpdate) {
        return null
    }
}
