package de.dkfz.roddy.execution.jobs.cluster

import com.google.common.reflect.TypeToken
import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.batcheuphoria.BETestBaseSpec
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.JobManagerOptions

import java.lang.reflect.Constructor

/**
 * Base test specification for job manager implementations.
 *
 * Used to enforce a common test structure for all subclasses.
 */
abstract class JobManagerImplementationBaseSpec<T extends BatchEuphoriaJobManager> extends BETestBaseSpec {

    protected static final BEJobID testJobID = new BEJobID("22005")

    /**
     * https://stackoverflow.com/questions/3403909/get-generic-type-of-class-at-runtime
     */
    private final TypeToken<T> typeToken = new TypeToken<T>(getClass()) {}

    private final Class<T> jobManagerClass = typeToken.getRawType()

    T _newJobManager(BEExecutionService testExecutionService, JobManagerOptions parms) {
        Constructor c = jobManagerClass.getConstructor(BEExecutionService, JobManagerOptions)
        return c.newInstance(testExecutionService, parms)
    }

    /**
     * Create a job manager instance of type T.
     * The job manager will feature a modified instance of BEExecutionService which will return
     * the contents of the resource file jobStateQueryResultFile when execute() is called.
     *
     * For this feature to work, your JobManager implementation must have a constructor with
     * BEExecutionService and JobManagerOptions as parameters.
     *
     * @param maxTrackingTime Duration used for job tracking filter tests
     * @param resourceFileForExecuteResult The file whose content will be returned for execute() calls
     * @return
     */
    final T createJobManagerWithModifiedExecutionService(String resourceFileForExecuteResult) {
        JobManagerOptions parms = JobManagerOptions.create().build()
        final Class<T> _class = jobManagerClass
        BEExecutionService testExecutionService = [
                execute: {
                    String s -> new ExecutionResult(true, 0, getResourceFile(_class, resourceFileForExecuteResult).readLines(), null)
                }
        ] as BEExecutionService
        return _newJobManager(testExecutionService, parms)
    }

    final T createJobManager() {
        JobManagerOptions parms = JobManagerOptions.create().build()
        TestExecutionService testExecutionService = new TestExecutionService("test", "test")
        return _newJobManager(testExecutionService, parms)
    }

    ///////////////////////////////////////////////////////////////////////////
    // Unfortunately it is not possible to override Spock test features.
    // Or at least, I was not able to find a suitable way. What I propose here
    // is, that we just list all necessary tests and hope for the best in the
    // future. Maybe the override support comes at some point. Until then,
    // we have a list in form of a big comment.
    ///////////////////////////////////////////////////////////////////////////

    /*
    abstract def "test submitJob"()

    abstract def "test addToListOfStartedJobs"()

    abstract def "test startHeldJobs"()

    abstract def "test killJobs"()

    // Most of the logic for the query(Extended)JobInfo methods is already done in the BatchEuphoriaJobManager spec
    // Concentrate on only a couple tests for query... in your JobManager test spec.
    abstract def "test queryJobInfoByJob (also tests multiple requests and by ID)"(String id, expectedState)

    abstract def "test queryAllJobInfo"()

    abstract def "test queryExtendedJobInfoByJob (also tests multiple requests and by ID)"(String id, expectedState)

    abstract def "test queryAllExtendedJobInfo"()

    abstract def "test getEnvironmentVariableGlobs"()

    abstract def "test getDefaultForHoldJobsEnabled"()

    abstract def "test isHoldJobsEnabled"() 

    abstract def "test getUserEmail"() 

    abstract def "test getUserMask"() 

    abstract def "test getUserGroup"() 

    abstract def "test getUserAccount"() 

    abstract def "test executesWithoutJobSystem"() 

    abstract def "test convertResourceSet"() 

    abstract def "test collectJobIDsFromJobs"() 

    abstract def "test extractAndSetJobResultFromExecutionResult"() 

    abstract def "test createCommand"() 

    abstract def "test parseJobID"() 

    abstract def "test parseJobState"() 

    abstract def "test executeStartHeldJobs"() 

    abstract def "test executeKillJobs"()
    */
}
