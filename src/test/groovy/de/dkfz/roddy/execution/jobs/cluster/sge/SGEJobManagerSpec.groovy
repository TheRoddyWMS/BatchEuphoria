/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.sge

import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.JobManagerImplementationBaseSpec

import static de.dkfz.roddy.execution.jobs.JobState.*

/**
 */
class SGEJobManagerSpec extends JobManagerImplementationBaseSpec<SGEJobManager> {


    def "test submitJob"() {
        return null
    }

    def "test addToListOfStartedJobs"() {
        return null
    }

    def "test startHeldJobs"() {
        return null
    }

    def "test killJobs"() {
        return null
    }

    def "test queryJobInfoByJob (also tests multiple requests and by ID)"(String id, expectedState) {
        given:
        def jobManager = createJobManagerWithModifiedExecutionService("simpleQStatOutput.txt")
        def jobID = new BEJobID(id)
        def job = new BEJob(jobID, jobManager)

        when:
        JobInfo info = jobManager.queryJobInfoByJob(job)

        then:
        info.jobID == jobID
        info.jobState == expectedState

        where:
        id      | expectedState
        "22000" | UNKNOWN
        "22003" | RUNNING
        "22005" | HOLD
    }

    def "test queryAllJobInfo"() {
        given:
        def jobManager = createJobManagerWithModifiedExecutionService("simpleQStatOutput.txt")
        def jobIDs = ["22003", "22005", "22006", "22007"].collect { new BEJobID(it) }
        when:
        def result = jobManager.queryAllJobInfo()

        then:
        result.size() == 4
        result.keySet() as List<BEJobID> == jobIDs
        result[jobIDs[0]].jobState == RUNNING
        result[jobIDs[1]].jobState == HOLD
        result[jobIDs[2]].jobState == HOLD
        result[jobIDs[3]].jobState == HOLD
    }

    SGEJobManager createSGEJobManagerForExtendedInfoTests() {
        // This one is special, as query extended states in SGE utilizes queryJobInfo
        BEExecutionService testExecutionService = [
                execute: {
                    String s ->
                        if (s.startsWith("qstat -xml"))
                            new ExecutionResult(true, 0, getResourceFile(SGEJobManagerSpec, "queryExtendedJobStatesWithQueuedJobs.xml").readLines(), null)
                        else
                            new ExecutionResult(true, 0, getResourceFile(SGEJobManagerSpec, "simpleQStatOutput.txt").readLines(), null)
                }
        ] as BEExecutionService
        def jobManager = new SGEJobManager(testExecutionService, new JobManagerOptionsBuilder().build())
        return jobManager
    }

    def "test queryExtendedJobInfoByJob (also tests multiple requests and by ID)"(String id, expectedState) {
        given:
        def jobManager = createSGEJobManagerForExtendedInfoTests()
        def jobID = new BEJobID(id)
        def job = new BEJob(jobID, jobManager)

        when:
        JobInfo info = jobManager.queryExtendedJobInfoByJob(job)

        then:
        info.jobID == jobID && info.jobState == expectedState

        where:
        id      | expectedState
        "22000" | UNKNOWN
        "22003" | RUNNING
        "22005" | HOLD
    }

    def "test queryAllExtendedJobInfo"() {
        given:
        def jobManager = createSGEJobManagerForExtendedInfoTests()
        def jobIDs = ["22003", "22005", "22006", "22007"].collect { new BEJobID(it) }

        when:
        Map<BEJobID, ExtendedJobInfo> result = jobManager.queryAllExtendedJobInfo()

        then:
        result.size() == 4
        result[jobIDs[0]].jobID == jobIDs[0]
        result[jobIDs[0]].jobState == RUNNING

        result[jobIDs[1]].jobID == jobIDs[1]
        result[jobIDs[1]].jobState == HOLD

        result[jobIDs[2]].jobID == jobIDs[2]
        result[jobIDs[2]].jobState == HOLD

        result[jobIDs[3]].jobID == jobIDs[3]
        result[jobIDs[3]].jobState == HOLD
    }

    def "test getEnvironmentVariableGlobs"() {
        return null
    }

    def "test getDefaultForHoldJobsEnabled"() {
        return null
    }

    def "test isHoldJobsEnabled"() {
        return null
    }

    def "test getUserEmail"() {
        return null
    }

    def "test getUserMask"() {
        return null
    }

    def "test getUserGroup"() {
        return null
    }

    def "test getUserAccount"() {
        return null
    }

    def "test executesWithoutJobSystem"() {
        return null
    }

    def "test convertResourceSet"() {
        return null
    }
//    @Test
//    public void testConvertToolEntryToPBSCommandParameters() {
    // Roddy specific tests!
//        ResourceSet rset1 = new ResourceSet(ResourceSetSize.l, new BufferValue(1, BufferUnit.G), 2, 1, new TimeUnit("h"), null, null, null);
//        ResourceSet rset2 = new ResourceSet(ResourceSetSize.l, null, null, 1, new TimeUnit("h"), null, null, null);
//        ResourceSet rset3 = new ResourceSet(ResourceSetSize.l, new BufferValue(1, BufferUnit.G), 2, null, null, null, null, null);
//
//        Configuration cfg = new Configuration(new InformationalConfigurationContent(null, Configuration.ConfigurationType.OTHER, "test", "", "", null, "", ResourceSetSize.l, null, null, null, null));
//
//        BatchEuphoriaJobManager cFactory = new SGEJobManager(false);
//        PBSResourceProcessingParameters test = (PBSResourceProcessingParameters) cFactory.convertResourceSet(cfg, rset1);
//        assert test.getProcessingCommandString().trim().equals("-V -l s_data=1024M");
//
//        test = (PBSResourceProcessingParameters) cFactory.convertResourceSet(cfg, rset2);
//        assert test.getProcessingCommandString().equals(" -V");
//
//        test = (PBSResourceProcessingParameters) cFactory.convertResourceSet(cfg, rset3);
//        assert test.getProcessingCommandString().equals(" -V -l s_data=1024M");
//    }

    def "test collectJobIDsFromJobs"() {
        return null
    }

    def "test extractAndSetJobResultFromExecutionResult"() {
        return null
    }

    def "test createCommand"() {
        return null
    }

    def "test parseJobID"() {
        return null
    }

    def "test parseJobState"() {
        return null
    }

    def "test executeStartHeldJobs"() {
        return null
    }

    def "test executeKillJobs"() {
        return null
    }
}
