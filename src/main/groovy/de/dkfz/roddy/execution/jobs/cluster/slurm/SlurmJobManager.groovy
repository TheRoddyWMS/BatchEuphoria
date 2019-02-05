/*
 * Copyright (c) 2018 German Cancer Research Center (DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.slurm

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedJobManager
import groovy.transform.CompileStatic

@CompileStatic
class SlurmJobManager extends GridEngineBasedJobManager {

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
    String getQueryCommandForJobInfo() {
        return null
    }

    @Override
    String getQueryCommandForExtendedJobInfo() {
        return null
    }

    @Override
    ExtendedJobInfo parseGenericJobInfo(String command) {
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
    protected JobState parseJobState(String stateString) {
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
    Map<BEJobID, ExtendedJobInfo> queryExtendedJobInfo(List jobIDs) {
        return null
    }
}