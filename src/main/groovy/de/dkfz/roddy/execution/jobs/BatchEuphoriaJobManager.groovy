/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeoutException

/**
 * Basic factory and manager class for BEJob and Command management
 * Currently supported are qsub via PBS or SGE. Other cluster systems or custom job submission / execution system are possible.
 *
 * @author michael
 */
@CompileStatic
abstract class BatchEuphoriaJobManager<C extends Command> {

    final static Logger log = LoggerFactory.getLogger(BatchEuphoriaJobManager)

    final BEExecutionService executionService

    protected boolean isTrackingOfUserJobsEnabled

    protected boolean queryOnlyStartedJobs

    protected String userIDForQueries

    private String userEmail

    private String userMask

    private String userGroup

    private String userAccount


    /**
     * Set this to true, if you do not want to allow any further job submission.
     */
    protected boolean forbidFurtherJobSubmission

    boolean requestMemoryIsEnabled
    boolean requestWalltimeIsEnabled
    boolean requestQueueIsEnabled
    boolean requestCoresIsEnabled
    boolean requestStorageIsEnabled
    boolean passEnvironment
    boolean holdJobsIsEnabled

    BatchEuphoriaJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        assert (executionService)
        this.executionService = executionService

        this.isTrackingOfUserJobsEnabled = parms.userIdForJobQueries as boolean
        this.queryOnlyStartedJobs = parms.trackOnlyStartedJobs
        this.userIDForQueries = parms.userIdForJobQueries

        this.userEmail = parms.userEmail
        this.userGroup = parms.userGroup
        this.userAccount = parms.userAccount
        this.userMask = parms.userMask

        this.requestMemoryIsEnabled = parms.requestMemoryIsEnabled
        this.requestWalltimeIsEnabled = parms.requestWalltimeIsEnabled
        this.requestQueueIsEnabled = parms.requestQueueIsEnabled
        this.requestCoresIsEnabled = parms.requestCoresIsEnabled
        this.requestStorageIsEnabled = parms.requestStorageIsEnabled
        this.passEnvironment = parms.passEnvironment
        this.holdJobsIsEnabled = Optional.ofNullable(parms.holdJobIsEnabled).orElse(getDefaultForHoldJobsEnabled())

    }

    /**
     * If you override this method, make sure to build in the check for further job submission! It is not allowed to
     * submit any jobs after waitForJobs() was called.
     * @param job
     * @return
     * @throws TimeoutException
     */
    BEJobResult submitJob(BEJob job) throws TimeoutException {
        if (forbidFurtherJobSubmission) {
            throw new BEException("You are not allowed to submit further jobs. This happens, when you call waitForJobs().")
        }
        Command command = createCommand(job)
        ExecutionResult executionResult = executionService.execute(command)
        extractAndSetJobResultFromExecutionResult(command, executionResult)
        addToListOfStartedJobs(job)
        return job.runResult
    }

    @Deprecated
    void addToListOfStartedJobs(BEJob job) {
    }

    /**
     * Resume given job
     * @param job
     */
    void startHeldJobs(List<BEJob> heldJobs) throws BEException {
        if (!isHoldJobsEnabled()) return
        if (!heldJobs) return
        List<BEJobID> jobIds = collectJobIDsFromJobs(heldJobs)
        if (jobIds) {
            ExecutionResult executionResult = executeStartHeldJobs(jobIds)
            if (!executionResult.successful) {
                String msg = "Held jobs couldn't be started.\n  status code: ${executionResult.exitCode}\n  result: ${executionResult.resultLines}"
                throw new BEException(msg)
            }
        }
    }

    static final assertListIsValid(List list) {
        assert list && list.every { it != null }
    }

    /**
     * Try to abort a list of jobs
     *
     * @param jobs
     */
    void killJobs(List<BEJob> jobs) {
        List<BEJobID> jobIds = collectJobIDsFromJobs(jobs)
        if (jobIds) {
            ExecutionResult executionResult = executeKillJobs(jobIds)
            if (executionResult.successful) {
                jobs.each { BEJob job -> job.jobState = JobState.ABORTED }
            } else {
                String msg = "Job couldn't be aborted.\n  status code: ${executionResult.exitCode}\n  result: ${executionResult.resultLines}"
                throw new BEException(msg)
            }
        }
    }

    final JobInfo queryJobInfoByJob(BEJob job) {
        assert job
        queryJobInfoByID(job.jobID)
    }

    final JobInfo queryJobInfoByID(BEJobID jobID) {
        assert jobID
        return queryJobInfoByID([jobID])[jobID]
    }

    /**
     * Queries the status of all jobs in the list.
     *
     * Every job in the list is supposed to have an entry in the result map. If
     * the manager cannot retrieve info about the job, the result will be UNKNOWN
     * for this particular job.
     *
     * @param jobs
     * @return
     */
    final Map<BEJob, JobInfo> queryJobInfoByJob(List<BEJob> jobs) {
        assertListIsValid(jobs)

        Map<BEJobID, BEJob> jobsByID = jobs.collectEntries {
            def job = it
            def id = it.jobID
            [id, job]
        }

        return queryJobInfoByID(jobs*.jobID)
                .collectEntries {
            BEJobID id, JobInfo info -> [jobsByID[id], info]
        } as Map<BEJob, JobInfo>
    }

    /**
     * Query a list of job states by their id.
     * If the status of a job could not be determined, the query will return UNKNOWN as the state for this job.
     *
     * @param jobIDs
     * @return A map of all job states accessible by their id.
     */
    final Map<BEJobID, JobInfo> queryJobInfoByID(List<BEJobID> jobIDs) {
        assertListIsValid(jobIDs)

        Map<BEJobID, JobInfo> result = queryJobInfo(jobIDs) ?: [:] as Map<BEJobID, JobInfo>

        // Collect the result and fill in empty entries with UNKNOWN
        return jobIDs.collectEntries {
            BEJobID jobID ->
                [jobID, result[jobID] ?: new JobInfo(jobID)]
        }
    }

    /**
     * Query the states of all jobs available limited by the settings provided in the constructor.
     *
     * @return
     */
    final Map<BEJobID, JobInfo> queryAllJobInfo() {
        queryJobInfo(null)
    }

    /**
     * Needs to be overriden by JobManager implementations.
     * jobIDs may be null or [] which means, that the query is not for specific jobs but for all available (except for
     * the filter settings of the JobManager) jobs.
     *
     * @param jobIDs A list of IDs OR null/[]
     */
    abstract Map<BEJobID, JobInfo> queryJobInfo(List<BEJobID> jobIDs)

    final ExtendedJobInfo queryExtendedJobInfoByJob(BEJob job) {
        assert job
        return queryExtendedJobInfoByID(job.jobID)
    }

    final ExtendedJobInfo queryExtendedJobInfoByID(BEJobID id) {
        assert id
        return queryExtendedJobInfoByID([id])[id]
    }

    /**
     * Will be used to gather extended information about a job like:
     * - The used memory
     * - The used cores
     * - The used walltime
     *
     * If the jobid cannot be found, an empty extend job info object will be returned with the jobstate set to UNKNOWN
     */
    final Map<BEJob, ExtendedJobInfo> queryExtendedJobInfoByJob(List<BEJob> jobs) {
        assertListIsValid(jobs)

        Map<BEJobID, BEJob> jobsByID = jobs.collectEntries { [it.jobID, it] }

        return queryExtendedJobInfoByID(jobs*.jobID)
                .collectEntries {
            BEJobID id, JobInfo info -> [jobsByID[id], info]
        } as Map<BEJob, ExtendedJobInfo>
    }

    /**
     * Will be used to gather extended information about a job like:
     * - The used memory
     * - The used cores
     * - The used walltime
     *
     * If the jobid cannot be found, an empty extend job info object will be returned with the jobstate set to UNKNOWN
     */
    final Map<BEJobID, ExtendedJobInfo> queryExtendedJobInfoByID(List<BEJobID> jobIDs) {
        assertListIsValid(jobIDs)

        Map<BEJobID, ExtendedJobInfo> queriedExtendedStates = queryExtendedJobInfo(jobIDs) ?: [:] as Map<BEJobID, ExtendedJobInfo>

        jobIDs.collectEntries {
            BEJobID jobID ->
                [
                        jobID,
                        queriedExtendedStates[jobID] ?: new ExtendedJobInfo(jobID)
                ]
        } as Map<BEJobID, ExtendedJobInfo>
    }

    final Map<BEJobID, ExtendedJobInfo> queryAllExtendedJobInfo() { return queryExtendedJobInfo(null) }

    /**
     * Needs to be overriden by JobManager implementations.
     * jobIDs may be null or [] which means, that the query is not for specific jobs but for all available (except for
     * the filter settings of the JobManager) jobs.
     *
     * @param jobIDs A list of IDs OR null/[]
     * @return a list of jobs
     */
    abstract Map<BEJobID, ExtendedJobInfo> queryExtendedJobInfo(List<BEJobID> jobIDs)

    abstract String getJobIdVariable()

    abstract String getJobNameVariable()

    abstract String getQueueVariable()

    abstract String getNodeFileVariable()

    abstract String getSubmitHostVariable()

    abstract String getSubmitDirectoryVariable()

    List<String> getEnvironmentVariableGlobs() {
        return Collections.unmodifiableList([] as List<String>)
    }

    boolean getDefaultForHoldJobsEnabled() { return true }

    boolean isHoldJobsEnabled() {
        return holdJobsIsEnabled
    }

    String getUserEmail() {
        return userEmail
    }

    String getUserMask() {
        return userMask
    }

    String getUserGroup() {
        return userGroup
    }

    String getUserAccount() {
        return userAccount
    }

    boolean executesWithoutJobSystem() {
        return false
    }

    abstract String getSubmissionCommand()

    abstract String getQueryCommandForJobInfo()

    abstract String getQueryCommandForExtendedJobInfo()

    ProcessingParameters convertResourceSet(BEJob job) {
        return convertResourceSet(job, job.resourceSet)
    }

    abstract ProcessingParameters convertResourceSet(BEJob job, ResourceSet resourceSet)

    /**
     * Tries to reverse assemble job information out of an executed command.
     * The format should be [id], [command, i.e. qsub...]
     *
     * @param commandString
     * @return
     */
    abstract ExtendedJobInfo parseGenericJobInfo(String command)

    protected List<BEJobID> collectJobIDsFromJobs(List<BEJob> jobs) {
        BEJob.jobsWithUniqueValidJobId(jobs).collect { it.runResult.getJobID() }
    }

    /**
     * Called by the execution service after a command was executed.
     */
    protected BEJobResult extractAndSetJobResultFromExecutionResult(Command command, ExecutionResult res) {
        BEJobResult jobResult
        if (res.successful) {
            String exID
            exID = parseJobID(res.resultLines.join("\n"))
            def job = command.getJob()
            BEJobID jobID = new BEJobID(exID)
            command.setJobID(jobID)
            job.resetJobID(jobID)
            jobResult = new BEJobResult(command, job, res, job.tool, job.parameters, job.parentJobs as List<BEJob>)
            job.setRunResult(jobResult)
        } else {
            def job = command.getJob()
            jobResult = new BEJobResult(command, job, res, job.tool, job.parameters, job.parentJobs as List<BEJob>)
            job.setRunResult(jobResult)
            throw new BEException("Job ${job.jobName ?: "NA"} could not be started. \n Returned status code:${res.exitCode} \n result:${res.resultLines}")
        }
        return jobResult
    }

    abstract protected Command createCommand(BEJob job)

    abstract protected String parseJobID(String commandOutput)

    abstract protected JobState parseJobState(String stateString)

    abstract protected ExecutionResult executeStartHeldJobs(List<BEJobID> jobIDs)

    abstract protected ExecutionResult executeKillJobs(List<BEJobID> jobIDs)


    @Deprecated
    Map<BEJob, JobState> queryJobStatus(List<BEJob> jobs) {
        queryJobInfoByJob(jobs).collectEntries { BEJob job, JobInfo ji -> [job, ji.jobState] } as Map<BEJob, JobState>
    }

    /**
     * Queries the status of all jobs in the list.
     *
     * Every job ID in the list is supposed to have an entry in the result map. If
     * the manager cannot retrieve info about the job, the result will be UNKNOWN
     * for this particular job.
     *
     * @param jobIds
     * @return
     */
    @Deprecated
    Map<BEJobID, JobState> queryJobStatusById(List<BEJobID> jobIds) {
        queryJobInfoByID(jobIds).collectEntries { BEJobID id, JobInfo ji -> [id, ji.jobState] } as Map<BEJobID, JobState>
    }

    /**
     * Queries the status of all jobs.
     *
     * @return
     */
    @Deprecated
    Map<BEJobID, JobState> queryJobStatusAll(boolean forceUpdate = false) {
        return queryAllJobInfo().collectEntries { BEJobID id, JobInfo ji -> [id, ji.jobState] } as Map<BEJobID, JobState>
    }

    @Deprecated
    Map<BEJob, ExtendedJobInfo> queryExtendedJobState(List<BEJob> jobs) {
        queryExtendedJobInfoByJob(jobs)
    }

    /**
     * Will be used to gather extended information about a job like:
     * - The used memory
     * - The used cores
     * - The used walltime
     *
     * @param jobIds
     * @return
     */
    @Deprecated
    Map<BEJobID, ExtendedJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds) {
        return queryExtendedJobInfo(jobIds)
    }


}
