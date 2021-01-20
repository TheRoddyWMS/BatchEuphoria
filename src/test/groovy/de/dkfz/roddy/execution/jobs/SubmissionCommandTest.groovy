/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.io.ExecutionResult
import spock.lang.Specification

class SubmissionCommandTest extends Specification {

    static BatchEuphoriaJobManager makeJobManager(final boolean passEnvironment) {
        return new BatchEuphoriaJobManager(
                TestHelper.makeExecutionService(),
                JobManagerOptions.create().setPassEnvironment(passEnvironment).build()) {

            @Override
            Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List jobIds) {
                return null
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
            String getQueryJobStatesCommand() {
                return null
            }

            @Override
            String getExtendedQueryJobStatesCommand() {
                return null
            }

            @Override
            ProcessingParameters convertResourceSet(BEJob job, ResourceSet resourceSet) {
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
            protected JobState parseJobState(String stateString) {
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
        }
    }

    def makeSubmissionCommand(final BatchEuphoriaJobManager jobManager, final Optional<Boolean> passEnvironment) {
        return new SubmissionCommand(jobManager, null, null, [], [:] as Map<String,String>, [], "") {

            {
                this.setPassEnvironment(passEnvironment)
            }

            @Override
            protected String getJobNameParameter() {
                return null
            }

            @Override
            protected String getHoldParameter() {
                return null
            }

            @Override
            protected String getAccountParameter(String account) {
                return null
            }

            @Override
            protected String getWorkingDirectoryParameter() {
                return null
            }

            @Override
            protected String getLoggingParameter(JobLog jobLog) {
                return null
            }

            @Override
            protected String getEmailParameter(String address) {
                return null
            }

            @Override
            protected String getGroupListParameter(String groupList) {
                return null
            }

            @Override
            protected String getUmaskString(String umask) {
                return null
            }

            @Override
            protected String assembleDependencyParameter(List<BEJobID> jobIds) {
                return null
            }

            @Override
            protected String assembleVariableExportParameters() throws BEException {
                return null
            }

            @Override
            protected String getAdditionalCommandParameters() {
                return null
            }
        }
    }

    def "GetPassLocalEnvironment_JobPrecedenceOverJobManager"() {
        when:
        def cmd = makeSubmissionCommand(makeJobManager(true), Optional.of(false))
        then:
        !cmd.getPassLocalEnvironment()
    }

    def "GetPassLocalEnvironment_JobManagerAsFallback"() {
        when:
        def cmd = makeSubmissionCommand(makeJobManager(true), Optional.empty())
        then:
        cmd.getPassLocalEnvironment()
    }
}
