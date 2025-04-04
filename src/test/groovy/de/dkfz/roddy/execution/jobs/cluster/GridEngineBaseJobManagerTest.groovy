package de.dkfz.roddy.execution.jobs.cluster

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.config.ResourceSetSize
import de.dkfz.roddy.tools.EscapableString
import de.dkfz.roddy.execution.Executable
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.execution.jobs.TestHelper
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic
import org.junit.Before
import org.junit.Test

import java.nio.file.Paths

import static de.dkfz.roddy.tools.EscapableString.Shortcuts.*

@CompileStatic
class GridEngineBaseJobManagerTest {

    GridEngineBasedJobManager jobManager

    @Before
    void setUp() throws Exception {
        jobManager = new GridEngineBasedJobManager(TestHelper.makeExecutionService(), JobManagerOptions.create().build()) {
            @Override
            void createComputeParameter(ResourceSet resourceSet, LinkedHashMultimap parameters) {

            }

            @Override
            void createQueueParameter(LinkedHashMultimap parameters, String queue) {

            }

            @Override
            void createWalltimeParameter(LinkedHashMultimap parameters, ResourceSet resourceSet) {

            }

            @Override
            void createMemoryParameter(LinkedHashMultimap parameters, ResourceSet resourceSet) {

            }

            @Override
            void createStorageParameters(LinkedHashMultimap parameters, ResourceSet resourceSet) {

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
        }
    }

    private BEJob makeJob(Map<String, EscapableString> mapOfParameters) {
        BEJob job = new BEJob
                (null,
                 jobManager,
                 u("Test"),
                 new Executable(Paths.get("/tmp/test.sh")),
                 new ResourceSet(ResourceSetSize.l,
                                 new BufferValue(1, BufferUnit.G),
                                 4,
                                 1,
                                 new TimeUnit("1h"),
                                 null,
                                 null,
                                 null),
                 [],
                 mapOfParameters)
        job
    }

    @Test
    void testAssembleDependencyStringWithoutDependencies() throws Exception {
        def mapOfVars = ["a": u("a"), "b": u("b")] as Map<String, EscapableString>
        GridEngineBasedSubmissionCommand cmd =
                new GridEngineBasedSubmissionCommand(
                        jobManager,
                        makeJob(mapOfVars),
                        u("jobName"),
                        null,
                        mapOfVars) {
            @Override
            protected String getDependsSuperParameter() {
                return null
            }

            @Override
            protected Boolean getQuoteCommand() {
                true
            }

            @Override
            protected String getDependencyParameterName() {
                return null
            }

            @Override
            protected String getDependencyOptionSeparator() {
                return null
            }

            @Override
            protected String getDependencyIDSeparator() {
                return null
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
            protected EscapableString getAdditionalCommandParameters() {
                return null
            }

            @Override
            protected EscapableString getEnvironmentString() {
                return u("")
            }

            @Override
            protected EscapableString assembleVariableExportParameters() {
                return null
            }

            @Override
            protected String composeCommandString(List<EscapableString> parameters) {
                return null
            }
        }
        assert cmd.assembleDependencyParameter([]) == c()
    }

}
