/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.tools.EscapableString
import de.dkfz.roddy.execution.io.ExecutionResult
import spock.lang.Specification

import java.time.Duration

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
            Command createCommand(BEJob job) {
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

            @Override
            Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List jobIds, Duration timeout) {
                return null
            }

            @Override
            protected Map<BEJobID, JobState> queryJobStates(List jobIDs, Duration timeout) {
                return null
            }
        }
    }

    def makeSubmissionCommand(final BatchEuphoriaJobManager jobManager, final Optional<Boolean> passEnvironment) {
        return new SubmissionCommand(
                jobManager,
                null,
                null as EscapableString,
                [],
                [:] as Map<String, EscapableString>
        ) {

            {
                this.setPassEnvironment(passEnvironment)
            }

            @Override
            String getSubmissionExecutableName() {
                return null
            }

            @Override
            protected Boolean getQuoteCommand() {
                true
            }

            @Override
            protected EscapableString getJobNameParameter() {
                return null
            }

            @Override
            protected EscapableString getHoldParameter() {
                return null
            }

            @Override
            protected EscapableString getWorkingDirectoryParameter() {
                return null
            }

            @Override
            protected EscapableString getLoggingParameter(JobLog jobLog) {
                return null
            }

            @Override
            protected EscapableString getEmailParameter(EscapableString address) {
                return null
            }

            @Override
            protected EscapableString getGroupListParameter(EscapableString groupList) {
                return null
            }

            @Override
            protected EscapableString getUmaskString(EscapableString umask) {
                return null
            }

            @Override
            protected EscapableString assembleDependencyParameter(List<BEJobID> jobIds) {
                return null
            }

            @Override
            protected EscapableString assembleVariableExportParameters() throws BEException {
                return null
            }

            @Override
            protected EscapableString getAdditionalCommandParameters() {
                return null
            }

            @Override
            protected EscapableString getEnvironmentString() {
                c()
            }

            @Override
            protected String composeCommandString(List<EscapableString> parameters) {
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
