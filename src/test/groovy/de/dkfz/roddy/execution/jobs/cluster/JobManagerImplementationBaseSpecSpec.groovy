package de.dkfz.roddy.execution.jobs.cluster

import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSJobManager

/**
 * Test spec for JobManagerImplementationBaseSpecSpec
 */
class JobManagerImplementationBaseSpecSpec extends JobManagerImplementationBaseSpec<PBSJobManager> {

    def "test createJobManagerWithModifiedExecutionService"() {
        when:
        BatchEuphoriaJobManager jm = createJobManagerWithModifiedExecutionService("createJobManagerWithModifiedExecutionServiceTest.txt")
        def expected = ["Line1", "Line2", "Line3"]
        def res0 = jm.executionService.execute("blabla").resultLines
        def res1 = jm.executionService.execute("something else").resultLines
        def res2 = jm.executionService.execute("just a test").resultLines

        then:
        res0 == expected
        res1 == expected
        res2 == expected
    }

    def "test createJobManager"() {
        when:
        BatchEuphoriaJobManager jm = createJobManager()

        then:
        jm instanceof PBSJobManager
        jm.getExecutionService() instanceof TestExecutionService
    }

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

    def "test queryJobStateByJob"() {
        return null
    }

    def "test queryJobStateByID"() {
        return null
    }

    def "test queryJobStatesByJob"() {
        return null
    }

    def "test queryJobStatesByID"() {
        return null
    }

    def "test queryAllJobStates"() {
        return null
    }

    def "test queryJobStates with overdue finish date"() {
        return null
    }

    def "test queryExtendedJobStatesByJob"() {
        return null
    }

    def "test queryExtendedJobStatesById"() {
        return null
    }

    def "test queryExtendedJobStatesById with overdue date"() {
        return null
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
