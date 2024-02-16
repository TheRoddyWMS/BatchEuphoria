/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ)..
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import com.google.common.base.Preconditions
import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.TimeoutException

/**
 * Basic factory and manager class for BEJob and Command management.
 * Currently supported are qsub via PBS or SGE. Other cluster systems or custom job submission / execution system are possible.
 *
 * TODO Refactor: Violates Single-Responsibility Principle (factory and manager) and parallel inherit. hierarchy with Command.
 * TODO Refactor: Co-/Contravariance works. This does not have to be a generic class. Remove the template.
 */
@CompileStatic
abstract class BatchEuphoriaJobManager<C extends Command> {

    final static Logger log = LoggerFactory.getLogger(BatchEuphoriaJobManager)

    protected final BEExecutionService executionService

    protected boolean isTrackingOfUserJobsEnabled

    protected boolean queryOnlyStartedJobs

    protected final Duration maxTrackingTimeForFinishedJobs

    protected String userIDForQueries

    private String userEmail

    private String userMask

    private String userGroup

    private String userAccount

    private final Map<BEJobID, BEJob> activeJobs = [:]

    private Thread updateDaemonThread

    /**
     * Set this to true to tell the job manager, that an existing update daemon shall be closed, e.g. because
     * the application is in the process to exit.
     */
    protected boolean updateDaemonShallBeClosed

    /**
     * Set this to true, if you do not want to allow any further job submission.
     */
    protected boolean forbidFurtherJobSubmission

    private Map<BEJobID, JobState> cachedStates = [:]
    private final Object cacheStatesLock = new Object()
    private LocalDateTime lastCacheUpdate
    private Duration cacheUpdateInterval

    public boolean surveilledJobsHadErrors = false

    private final List<UpdateDaemonListener> updateDaemonListeners = []

    boolean requestMemoryIsEnabled
    boolean requestWalltimeIsEnabled
    boolean requestQueueIsEnabled
    boolean requestCoresIsEnabled
    boolean requestStorageIsEnabled
    boolean passEnvironment
    boolean holdJobsIsEnabled
    Duration commandTimeout

    BatchEuphoriaJobManager(BEExecutionService executionService,
                            JobManagerOptions parms) {
        Preconditions.checkArgument(executionService != null)
        this.executionService = executionService

        this.isTrackingOfUserJobsEnabled = parms.userIdForJobQueries as boolean
        this.queryOnlyStartedJobs = parms.trackOnlyStartedJobs
        this.maxTrackingTimeForFinishedJobs = parms.maxTrackingTimeForFinishedJobs
        this.userIDForQueries = parms.userIdForJobQueries

        this.userEmail = parms.userEmail
        this.userGroup = parms.userGroup
        this.userAccount = parms.userAccount
        this.userMask = parms.userMask
        this.cacheUpdateInterval = parms.updateInterval

        this.requestMemoryIsEnabled = parms.requestMemoryIsEnabled
        this.requestWalltimeIsEnabled = parms.requestWalltimeIsEnabled
        this.requestQueueIsEnabled = parms.requestQueueIsEnabled
        this.requestCoresIsEnabled = parms.requestCoresIsEnabled
        this.requestStorageIsEnabled = parms.requestStorageIsEnabled
        this.passEnvironment = parms.passEnvironment
        this.holdJobsIsEnabled = Optional.ofNullable(parms.holdJobIsEnabled).orElse(getDefaultForHoldJobsEnabled())
        this.commandTimeout = parms.commandTimeout

        if (parms.createDaemon) {
            createUpdateDaemonThread()
        }
    }

    void setQueryJobStatesFilter() {

    }

    /**
     * If you override this method, make sure to build in the check for further job submission! It is not allowed to
     * submit any jobs after waitForJobs() was called.
     * @param job
     * @return
     * @throws TimeoutException
     */
    BEJobResult submitJob(BEJob job)
            throws TimeoutException, BEException {
        if (forbidFurtherJobSubmission) {
            throw new BEException(
                    "You are not allowed to submit further jobs." +
                    "You called waitForJobs() before.")
        }
        Command command = createCommand(job)
        ExecutionResult executionResult = executionService.execute(command, commandTimeout)
        extractAndSetJobResultFromExecutionResult(command, executionResult)
        addToListOfStartedJobs(job)
        return job.runResult
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
                String msg = "Held jobs couldn't be started. ${executionResult.toStatusLine()}"
                throw new BEException(msg)
            }
        }
    }

    /**
     * Try to abort a list of jobs
     * @param jobs
     */
    void killJobs(List<BEJob> jobs) {
        List<BEJobID> jobIds = collectJobIDsFromJobs(jobs)
        if (jobIds) {
            ExecutionResult executionResult = executeKillJobs(jobIds)
            if (executionResult.successful) {
                jobs.each { BEJob job -> job.jobState = JobState.ABORTED }
            } else {
                String msg = "Job couldn't be aborted. ${executionResult.toStatusLine()}"
                throw new BEException(msg)
            }
        }
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
    Map<BEJob, JobState> queryJobStatus(List<BEJob> jobs, boolean forceUpdate = false,
                                        Duration timeout = Duration.ZERO) {
        Map<BEJobID, JobState> result = queryJobStatesUsingCache(jobs*.jobID, forceUpdate, timeout)
        return jobs.collectEntries { BEJob job ->
            [job, job.jobState == JobState.ABORTED ? JobState.ABORTED :
                    result[job.jobID] ?: JobState.UNKNOWN]
        }
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
    Map<BEJobID, JobState> queryJobStatusById(List<BEJobID> jobIds, boolean forceUpdate = false,
                                              Duration timeout = Duration.ZERO) {
        Map<BEJobID, JobState> result = queryJobStatesUsingCache(jobIds, forceUpdate, timeout)
        return jobIds.collectEntries { BEJobID jobId -> [jobId, result[jobId] ?: JobState.UNKNOWN] }
    }

    /**
     * Queries the status of all jobs.
     *
     * @return
     */
    Map<BEJobID, JobState> queryJobStatusAll(boolean forceUpdate = false,
                                             Duration timeout = Duration.ZERO) {
        return queryJobStatesUsingCache(null, forceUpdate, timeout)
    }

    /**
     * Will be used to gather extended information about a job like:
     * - The used memory
     * - The used cores
     * - The used walltime
     *
     * @param jobs
     * @return
     */
    Map<BEJob, GenericJobInfo> queryExtendedJobState(List<BEJob> jobs,
                                                     Duration timeout = Duration.ZERO) {

        Map<BEJobID, GenericJobInfo> queriedExtendedStates = queryExtendedJobStateById(
                jobs.collect { it.jobID },
                timeout)
        return (Map<BEJob, GenericJobInfo>) queriedExtendedStates.collectEntries {
            Map.Entry<BEJobID, GenericJobInfo> it ->
                [jobs.find { BEJob temp -> temp.jobID == it.key }, (GenericJobInfo) it.value]
        }
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
    abstract Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(
            List<BEJobID> jobIds,
            Duration timeout = Duration.ZERO)

    void addToListOfStartedJobs(BEJob job) {
        if (updateDaemonThread) {
            synchronized (activeJobs) {
                activeJobs.put(job.getJobID(), job)
            }
        }
    }

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

    abstract String getQueryJobStatesCommand()

    abstract String getExtendedQueryJobStatesCommand()

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
    abstract GenericJobInfo parseGenericJobInfo(String command)

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
            exID = parseJobID(res.stdout.join("\n"))
            def job = command.getJob()
            BEJobID jobID = new BEJobID(exID)
            command.setJobID(jobID)
            job.resetJobID(jobID)
            jobResult = new BEJobResult(command, job, res, job.parameters, job.parentJobs as List<BEJob>)
            job.setRunResult(jobResult)
            synchronized (cacheStatesLock) {
                cachedStates.put(jobID, isHoldJobsEnabled() ? JobState.HOLD : JobState.QUEUED)
            }
        } else {
            def job = command.getJob()
            jobResult = new BEJobResult(command, job, res, job.parameters, job.parentJobs as List<BEJob>)
            job.setRunResult(jobResult)
            throw new BEException("Job ${job.jobName ?: "NA"} could not be started. ${res.toStatusLine()}")
        }
        return jobResult
    }

    // This needs to be public, because the RoddyCore's NativeWorkflow breaks the abstraction
    // (not just by calling this, but by actually messing with the JobManager interna).
    abstract Command createCommand(BEJob job)

    abstract protected String parseJobID(String commandOutput)

    abstract protected JobState parseJobState(String stateString)

    abstract protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs,
                                                             Duration timeout = Duration.ZERO)

    abstract protected ExecutionResult executeStartHeldJobs(List<BEJobID> jobIDs)

    abstract protected ExecutionResult executeKillJobs(List<BEJobID> jobIDs)

    protected void createUpdateDaemonThread() {
        updateDaemonThread = Thread.startDaemon("Job state update daemon.", {
            while (!updateDaemonShallBeClosed) {
                updateActiveJobList()

                waitForUpdateIntervalDuration()
            }
        })
    }

    final void addUpdateDaemonListener(UpdateDaemonListener listener) {
        synchronized (updateDaemonListeners) {
            updateDaemonListeners << listener
        }
    }

    boolean isDaemonAlive() {
        return updateDaemonThread != null && updateDaemonThread.isAlive()
    }

    void waitForUpdateIntervalDuration() {
        long duration = Math.max(cacheUpdateInterval.toMillis(), 10 * 1000)
        // Sleep one second until the duration is reached. This allows the daemon to finish faster, when it shall stop
        // (updateDaemonShallBeClosed == true)
        for (long timer = duration; timer > 0 && !updateDaemonShallBeClosed; timer -= 1000)
            Thread.sleep(1000)
    }

    void stopUpdateDaemon() {
        updateDaemonShallBeClosed = true
        updateDaemonThread?.join()
    }

    private void updateActiveJobList() {
        List<BEJobID> listOfRemovableJobs = []
        synchronized (activeJobs) {
            Map<BEJobID, JobState> states = queryJobStatesUsingCache(activeJobs.keySet() as List<BEJobID>, true)

            for (BEJobID id : activeJobs.keySet()) {
                JobState jobState = states.get(id)
                BEJob job = activeJobs.get(id)

                job.setJobState(jobState)
                if (!(jobState.planned || jobState.running)) {
                    synchronized (updateDaemonListeners) {
                        updateDaemonListeners.each { it.jobEnded(job, jobState) }
                    }
                    if (!jobState.successful) {
                        surveilledJobsHadErrors = true
                    }
                    listOfRemovableJobs << id
                }
            }
            listOfRemovableJobs.each { activeJobs.remove(it) }
        }
    }

    private Map<BEJobID, JobState> queryJobStatesUsingCache(List<BEJobID> jobIDs,
                                                            boolean forceUpdate,
                                                            Duration timeout = Duration.ZERO) {
        if (forceUpdate || lastCacheUpdate == null || cacheUpdateInterval == Duration.ZERO ||
                Duration.between(lastCacheUpdate, LocalDateTime.now()) > cacheUpdateInterval) {
            synchronized (cacheStatesLock) {
                cachedStates = queryJobStates(jobIDs, timeout)
            }
            lastCacheUpdate = LocalDateTime.now()
        }
        return new HashMap(cachedStates)
    }

    /**
     * The method will wait until all started jobs are finished (with or without errors).
     *
     * Note, that the method does not allow further job submission! As soon, as you call it, you cannot submit jobs!
     *
     * @return true, if there were NO errors, false, if there were any.
     */
    boolean waitForJobsToFinish() {
        if (!updateDaemonThread) {
            throw new BEException("The job manager must be created with JobManagerOption.createDaemon set to true to make waitForJobsToFinish() work.")
        }
        forbidFurtherJobSubmission = true
        while (!updateDaemonShallBeClosed) {
            synchronized (activeJobs) {
                if (activeJobs.isEmpty()) {
                    break
                }
            }
            waitForUpdateIntervalDuration()
        }
        return !surveilledJobsHadErrors
    }
}
