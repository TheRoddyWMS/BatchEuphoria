import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.execution.jobs.cluster.ClusterJobManager
import groovy.transform.CompileStatic

/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

@CompileStatic
class SlurmJobManager extends ClusterJobManager {

    SlurmJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    @Override
    String getJobIdVariable() {
        return null
    }

    @Override
    String getJobNameVariable() {
        return null
    }

    @Override
    String getQueueVariable() {
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
    String getSubmissionCommand() {
        return null
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String command) {
        return null
    }

    @Override
    protected Command createCommand(BEJob job) {
        return null
    }

    @Override
    protected String parseJobID(String commandOutput) {
        return null
    }

    @Override
    protected ExecutionResult executeKillJobs(List jobIDs) {
        return null
    }

    @Override
    protected ExecutionResult executeStartHeldJobs(List jobIDs) {
        return null
    }

    @Override
    protected Map<BEJobID, JobState> queryJobStates(List jobIDs) {
        return null
    }

    @Override
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List jobIds) {
        return null
    }

    @Override
    void createStorageParameters(LinkedHashMultimap parameters, ResourceSet resourceSet) {

    }

    @Override
    void createMemoryParameter(LinkedHashMultimap parameters, ResourceSet resourceSet) {

    }

    @Override
    void createWalltimeParameter(LinkedHashMultimap parameters, ResourceSet resourceSet) {

    }

    @Override
    void createQueueParameter(LinkedHashMultimap parameters, String queue) {

    }

    @Override
    void createComputeParameter(ResourceSet resourceSet, LinkedHashMultimap parameters) {

    }

    @Override
    void createDefaultManagerParameters(LinkedHashMultimap parameters) {

    }
}