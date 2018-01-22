package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import spock.lang.Specification
import de.dkfz.roddy.execution.jobs.SubmissionCommand.PassEnvironmentVariables as PassVars

class SubmissionCommandTest extends Specification {

    def makeSubmissionCommand(final BatchEuphoriaJobManager jobManager, final Optional<PassVars> passEnvironment) {
        return new SubmissionCommand(jobManager, null, null, [:]) {

            {
                super.passEnvironment = passEnvironment
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

    BEExecutionService makeExecutionService() {
        return new BEExecutionService() {
            @Override
            ExecutionResult execute(Command command) {
                return null
            }

            @Override
            ExecutionResult execute(Command command, boolean waitFor) {
                return null
            }

            @Override
            ExecutionResult execute(String command) {
                return null
            }

            @Override
            ExecutionResult execute(String command, boolean waitFor) {
                return null
            }

            @Override
            ExecutionResult execute(String command, boolean waitForIncompatibleClassChangeError, OutputStream outputStream) {
                return null
            }

            @Override
            boolean isAvailable() {
                return false
            }

            @Override
            File queryWorkingDirectory() {
                return null
            }
        }
    }

    def makeJobManager(final Optional<PassVars> passEnvironment) {
        return new BatchEuphoriaJobManager<SubmissionCommand>(
                makeExecutionService(),
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
        cmd.getPassLocalEnvironment() == PassVars.Requested
    }

    def "GetPassLocalEnvironment_JobPrecedenceOverJobManager"() {
        when:
        def cmd = makeSubmissionCommand(makeJobManager(Optional.of(PassVars.All)), Optional.of(PassVars.None))
        then:
        cmd.getPassLocalEnvironment() == PassVars.None
    }

    def "GetPassLocalEnvironment_JobManagerAsFallback"() {
        when:
        def cmd = makeSubmissionCommand(makeJobManager(Optional.of(PassVars.None)), Optional.empty())
        then:
        cmd.getPassLocalEnvironment() == PassVars.None
    }
}
