package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.execution.jobs.cluster.JobManagerImplementationBaseSpec
import spock.lang.Shared

import static de.dkfz.roddy.execution.jobs.JobState.*

class BatchEuphoriaJobManagerSpec extends JobManagerImplementationBaseSpec {

    /**
     * We will only test non abstract methods in the BatchEuphoriaJobManager class (which is abstract for reasons)
     * For this, we will first create a custom test class, overriding some special methods necessary for our test
     * cases. Afterwards, we create a Spy() of this class, which can then be utilized in our tests.
     */
    abstract class TestBEJobManager extends BatchEuphoriaJobManager {
        TestBEJobManager() {
            super(
                    new TestExecutionService("test", "test"),
                    new JobManagerOptionsBuilder().build()
            )
        }

        @Override
        Map<BEJobID, JobInfo> queryJobInfo(List jobIDs) {
            // For the tests, it actually does not matter if the result contains JobInfo or ExtendedJobInfo objects
            return queryExtendedJobInfo(jobIDs) as Map<BEJobID, JobInfo>
        }

        @Override
        Map<BEJobID, ExtendedJobInfo> queryExtendedJobInfo(List jobIDs) {
            [
                    "1000": RUNNING,
                    "1001": SUSPENDED,
                    "1002": COMPLETED_SUCCESSFUL,
                    "1003": FAILED,
            ].collectEntries {
                String _id, JobState state ->
                    BEJobID id = new BEJobID(_id)
                    [id, new ExtendedJobInfo(id, state)]
            } as Map<BEJobID, ExtendedJobInfo>
        }
    }

    def "test assertListIsValid"(list) {
        when:
        BatchEuphoriaJobManager.assertListIsValid(list)

        then:
        thrown(AssertionError)

        where:
        list   | _
        null   | _
        []     | _
        [null] | _
    }

    @Shared
    TestBEJobManager jobManager = Spy()

    def "test queryJobInfoByJob (query for single object, also tests query by ID!)"(String id, JobState expectedState) {
        when:
        JobInfo result = jobManager.queryJobInfoByJob(new BEJob(new BEJobID(id), jobManager))

        then:
        result          // There is ALWAYS a result!
        result.jobState == expectedState

        where:
        id     | expectedState
        "999"  | UNKNOWN
        "1000" | RUNNING
        "1001" | SUSPENDED
        "1002" | COMPLETED_SUCCESSFUL
        "1003" | FAILED
    }

    def "test queryJobInfoByJob"(String id, JobState expectedState) {
        given:
        def jobID = new BEJobID(id)
        def job = new BEJob(jobID, jobManager)

        when:
        def result = jobManager.queryJobInfoByJob([job])

        then:
        result.size() == 1
        result[job].jobState == expectedState

        where:
        id     | expectedState
        "999"  | UNKNOWN
        "1000" | RUNNING
    }

    def "test queryJobInfoByJob with null id or empty list"(List input) {

        when:
        jobManager.queryJobInfoByJob(input)

        then:
        thrown(AssertionError)

        where:
        input  | _
        null   | _
        []     | _
        [null] | _
    }

    def "test queryAllJobInfo (same like above with null or [])"() {
        expect:
        jobManager.queryAllJobInfo().size() == 4
    }

    def "test queryExtendedJobInfoByJob (query for single object, also tests query by ID!)"() {
        when:
        JobInfo result = jobManager.queryExtendedJobInfoByJob(new BEJob(new BEJobID(id), jobManager))

        then:
        result          // There is ALWAYS a result!
        result.jobState == expectedState

        where:
        id     | expectedState
        "999"  | UNKNOWN
        "1000" | RUNNING
        "1001" | SUSPENDED
        "1002" | COMPLETED_SUCCESSFUL
        "1003" | FAILED
    }

    def "test queryExtendedJobInfoByJob (will also test query by id"() {
        given:
        def jobID = new BEJobID(id)
        def job = new BEJob(jobID, jobManager)

        when:
        def result = jobManager.queryExtendedJobInfoByJob([job])

        then:
        result.size() == 1
        result[job].jobState == expectedState

        where:
        id     | expectedState
        "999"  | UNKNOWN
        "1000" | RUNNING
        "1001" | SUSPENDED
        "1002" | COMPLETED_SUCCESSFUL
        "1003" | FAILED
    }

    def "test queryExtendedJobInfoByJob with null id or empty list"(List input) {

        when:
        jobManager.queryExtendedJobInfoByJob(input)

        then:
        thrown(AssertionError)

        where:
        input  | _
        null   | _
        []     | _
        [null] | _
    }

    def "test queryAllExtendedJobInfo"() {
        expect:
        jobManager.queryAllExtendedJobInfo().size() == 4
    }
}
