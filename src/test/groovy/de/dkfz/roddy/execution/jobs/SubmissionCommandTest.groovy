package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.io.NoNoExecutionService
import spock.lang.Specification

class SubmissionCommandTest extends Specification {

    def makeSubmissionCommand(final BatchEuphoriaJobManager jobManager, final Optional<Boolean> passEnvironment) {
        return new SubmissionCommand(jobManager, null, null, [:]) {

            {
                this.passEnvironment = passEnvironment
            }

            @Override
            protected String getEnvironmentExportParameter() {
                return null
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
            protected String getWorkingDirectory() {
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
            protected String assembleDependencyString(List<BEJobID> jobIds) {
                return null
            }

            @Override
            protected String assembleVariableExportString() {
                return null
            }

            @Override
            protected String getAdditionalCommandParameters() {
                return null
            }
        }
    }

    def makeJobManager(final Optional<Boolean> passEnvironment) {
        return new BatchEuphoriaJobManager<SubmissionCommand>(
                new NoNoExecutionService(),
                JobManagerOptions.create().setPassEnvironment(passEnvironment).build()) {

            { // instance initializer
                this.passEnvironment = passEnvironment
            }

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

    def "GetPassLocalEnvironment_BothUnset"() {
        when:
        def cmd = makeSubmissionCommand(makeJobManager(Optional.empty()), Optional.empty())
        then:
        cmd.getPassLocalEnvironment() == false
    }

    def "GetPassLocalEnvironment_JobPrecedenceOverJobManager"() {
        when:
        def cmd = makeSubmissionCommand(makeJobManager(Optional.of(false)), Optional.of(true))
        then:
        cmd.getPassLocalEnvironment() == false
    }

    def "GetPassLocalEnvironment_JobManagerAsFallback"() {
        when:
        def cmd = makeSubmissionCommand(makeJobManager(Optional.of(true)), Optional.empty())
        then:
        cmd.getPassLocalEnvironment() == true
    }
}
