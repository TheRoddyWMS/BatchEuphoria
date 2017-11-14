/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf.rest

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.ClusterJobManager
import de.dkfz.roddy.execution.jobs.cluster.lsf.LSFCommand
import de.dkfz.roddy.tools.BufferUnit
import groovy.transform.CompileStatic
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import java.time.Duration

/**
 * Created by kaercher on 22.03.17.
 */
@CompileStatic
class BatchEuphoriaJobManagerAdapter extends ClusterJobManager<LSFCommand> {

    BatchEuphoriaJobManagerAdapter(BEExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms)
    }

    @Override
    String getJobIdVariable() {
        return "LSB_JOBID"
    }

    @Override
    String getJobNameVariable() {
        return "LSB_JOBNAME"
    }

    @Override
    String getQueueVariable() {
        return 'LSB_QUEUE'
    }

    @Override
    String getNodeFileVariable() {
        return "LSB_HOSTS"
    }

    @Override
    String getSubmitHostVariable() {
        return "LSB_SUB_HOST"
    }

    @Override
    String getSubmitDirectoryVariable() {
        return "LSB_SUBCWD"
    }

    @Override
    BEJobResult runJob(BEJob job) {
        throw new NotImplementedException()
    }

    // needed
    @Override
    ProcessingParameters convertResourceSet(BEJob job, ResourceSet resourceSet) {
        LinkedHashMultimap<String, String> resourceParameters = LinkedHashMultimap.create()
        if (resourceSet.isQueueSet()) {
            resourceParameters.put('-q', resourceSet.getQueue())
        }
        if (resourceSet.isMemSet()) {
            String memo = resourceSet.getMem().toString(BufferUnit.M)
            resourceParameters.put('-M', memo.substring(0, memo.toString().length() - 1))
        }
        if (resourceSet.isWalltimeSet()) {
            resourceParameters.put('-W', durationToLSFWallTime(resourceSet.getWalltimeAsDuration()))
        }
        if (resourceSet.isCoresSet() || resourceSet.isNodesSet()) {
            int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
            resourceParameters.put('-n', nodes.toString())
        }
        return new ProcessingParameters(resourceParameters)
    }

    private String durationToLSFWallTime(Duration wallTime) {
        if (wallTime) {
            return String.valueOf(wallTime.toMinutes())
        }
        return null
    }

    @Override
    ProcessingParameters extractProcessingParametersFromToolScript(File file) {
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

// needed
    @Override
    void queryJobAbortion(List executedJobs) {
        throw new NotImplementedException()
    }

    // needed
    @Override
    Map<String, JobState> queryJobStatus(List jobs) {
        throw new NotImplementedException()
    }

    @Override
    LSFCommand createCommand(BEJob job, String jobName, List processingCommands, File tool, Map parameters, List parentJobs) {
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
