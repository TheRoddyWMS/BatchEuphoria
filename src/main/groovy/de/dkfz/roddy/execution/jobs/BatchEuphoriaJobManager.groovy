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
import de.dkfz.roddy.tools.LoggerWrapper
import groovy.transform.CompileStatic

import java.time.Duration
import java.time.LocalDateTime

/**
 * Basic factory and manager class for BEJob and Command management
 * Currently supported are qsub via PBS or SGE. Other cluster systems or custom job submission / execution system are possible.
 *
 * @author michael
 */
@CompileStatic
abstract class BatchEuphoriaJobManager<C extends Command> {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(BatchEuphoriaJobManager.class.getSimpleName())

    protected final BEExecutionService executionService

    protected boolean isTrackingOfUserJobsEnabled

    protected boolean queryOnlyStartedJobs

    protected String userIDForQueries

    protected boolean strictMode

    private String userEmail

    private String userMask

    private String userGroup

    private String userAccount

    private Boolean isHoldJobsEnabled = null

    private final Map<BEJobID, BEJob> startedJobs = [:]
    private Thread updateDaemonThread

    private Map<BEJobID, JobState> cachedStates = [:]
    private final Object cacheStatesLock = new Object()
    private LocalDateTime lastCacheUpdate
    private Duration cacheUpdateInterval


    BatchEuphoriaJobManager(BEExecutionService executionService, JobManagerCreationParameters parms) {
        this.executionService = executionService

        this.isTrackingOfUserJobsEnabled = parms.trackUserJobsOnly
        this.queryOnlyStartedJobs = parms.trackOnlyStartedJobs
        this.userIDForQueries = parms.userIdForJobQueries
        if (!userIDForQueries && isTrackingOfUserJobsEnabled) {
            logger.warning("Silently falling back to default job tracking behaviour. The user name was not set properly and the system cannot track the users jobs.")
            isTrackingOfUserJobsEnabled = false
        }
        this.userEmail = parms.userEmail
        this.userGroup = parms.userGroup
        this.userAccount = parms.userAccount
        this.userMask = parms.userMask
        this.strictMode = parms.strictMode
        this.cacheUpdateInterval = Duration.ofSeconds(parms.updateInterval)

        if (parms.createDaemon) {
            createUpdateDaemonThread()
        }
    }

    protected void createUpdateDaemonThread() {
        try {
            updateDaemonThread = Thread.startDaemon("Job state update daemon.", {
                updateJobsInStartedJobsList()
                try {
                    Thread.sleep(Math.max(cacheUpdateInterval.toMillis(), 10*1000))
                } catch (InterruptedException e) {
                    e.printStackTrace()
                }
            })
        } catch (Exception ex) {
            logger.severe("Creating the job state update daemon failed. BE will not be able to query the job system.", ex)
        }
    }

    abstract BEJobResult runJob(BEJob job)

    /**
     * Called by the execution service after a command was executed.
     */
    BEJobResult extractAndSetJobResultFromExecutionResult(Command command, ExecutionResult res) {
        BEJobResult jobResult
        if (res.successful) {
            String exID = parseJobID(res.resultLines[0])
            def job = command.getJob()
            BEJobID jobID = new BEJobID(exID)
            command.setExecutionID(jobID)
            job.resetJobID(jobID)
            jobResult = new BEJobResult(command, job, res, job.tool, job.parameters, job.parentJobs as List<BEJob>)
            job.setRunResult(jobResult)
            synchronized (cacheStatesLock) {
                cachedStates.put(jobID, isHoldJobsEnabled ? JobState.HOLD : JobState.QUEUED)
            }
        } else {
            def job = command.getJob()
            jobResult = new BEJobResult(command, job, res, job.tool, job.parameters, job.parentJobs as List<BEJob>)
            job.setRunResult(jobResult)
            logger.postAlwaysInfo("Job ${job.jobName?:"NA"} could not be started. \n Returned status code:${res.exitCode} \n result:${res.resultLines}")
            throw new BEException("Job ${job.jobName?:"NA"} could not be started. \n Returned status code:${res.exitCode} \n result:${res.resultLines}")
        }
        return jobResult
    }

    void startHeldJobs(List<BEJob> jobs) {}

    boolean getDefaultForHoldJobsEnabled() { return false }

    boolean isHoldJobsEnabled() { return isHoldJobsEnabled ?: getDefaultForHoldJobsEnabled() }

    ProcessingParameters convertResourceSet(BEJob job) {
        return convertResourceSet(job, job.resourceSet)
    }

    abstract ProcessingParameters convertResourceSet(BEJob job, ResourceSet resourceSet)

    abstract ProcessingParameters extractProcessingParametersFromToolScript(File file)

    /**
     * Tries to reverse assemble job information out of an executed command.
     * The format should be [id], [command, i.e. qsub...]
     *
     * @param commandString
     * @return
     */
    abstract GenericJobInfo parseGenericJobInfo(String command)

    private void updateJobsInStartedJobsList() {
        synchronized (startedJobs) {
            Map<BEJobID, JobState> states = queryJobStatesUsingCache(startedJobs.keySet() as List<BEJobID>, true)

            for (BEJobID id : startedJobs.keySet()) {
                JobState js = states.get(id)
                BEJob job = startedJobs.get(id)

                if (job.getJobState() in [JobState.FAILED, JobState.COMPLETED_SUCCESSFUL, JobState.COMPLETED_UNKNOWN, JobState.ABORTED]){
                    startedJobs.remove(id)
                    continue
                }

                job.setJobState(js)
            }
        }
    }


    abstract protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs)

    private Map<BEJobID, JobState> queryJobStatesUsingCache(List<BEJobID> jobIDs, boolean forceUpdate) {
        if (forceUpdate || lastCacheUpdate == null || cacheUpdateInterval == Duration.ZERO ||
                Duration.between(lastCacheUpdate, LocalDateTime.now()) > cacheUpdateInterval) {
            synchronized (cacheStatesLock) {
                cachedStates = queryJobStates(jobIDs)
            }
            lastCacheUpdate = LocalDateTime.now()
        }
        return new HashMap(cachedStates)
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
    Map<BEJob, JobState> queryJobStatus(List<BEJob> jobs, boolean forceUpdate = false) {
        Map<BEJobID, JobState> result = queryJobStatesUsingCache(jobs*.jobID, forceUpdate)
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
    Map<BEJobID, JobState> queryJobStatusById(List<BEJobID> jobIds, boolean forceUpdate = false) {
        Map<BEJobID, JobState> result = queryJobStatesUsingCache(jobIds, forceUpdate)
        return jobIds.collectEntries { BEJobID jobId -> [jobId, result[jobId] ?: JobState.UNKNOWN] }
    }

    /**
     * Queries the status of all jobs.
     *
     * @return
     */
    Map<BEJobID, JobState> queryJobStatusAll(boolean forceUpdate = false) {
        return queryJobStatesUsingCache(null, forceUpdate)
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
    Map<BEJob, GenericJobInfo> queryExtendedJobState(List<BEJob> jobs) {

        Map<BEJobID, GenericJobInfo> queriedExtendedStates = queryExtendedJobStateById(jobs.collect { it.getJobID() })
        return (Map<BEJob, GenericJobInfo>) queriedExtendedStates.collectEntries {
            Map.Entry<BEJobID, GenericJobInfo> it -> [jobs.find { BEJob temp -> temp.getJobID() == it.key }, (GenericJobInfo) it.value]
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
    abstract Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds)

    /**
     * Try to abort a range of jobs
     * @param executedJobs
     */
    abstract void queryJobAbortion(List<BEJob> executedJobs)

    void addToListOfStartedJobs(BEJob job) {
        if (updateDaemonThread) {
            synchronized (startedJobs) {
                startedJobs.put(job.getJobID(), job)
            }
        }
    }

    abstract String getLogFileWildcard(BEJob job)

    int waitForJobsToFinish() {
        logger.info("The user requested to wait for all jobs submitted by this process to finish.")
        if (!updateDaemonThread) {
            throw new BEException("createDaemon needs to be enabled for waitForJobsToFinish")
        }
        while(true) {
            synchronized (startedJobs) {
                if (startedJobs.isEmpty()) {
                    return 0
                }
            }
            Thread.sleep(cacheUpdateInterval.toMillis())
        }
    }

    abstract String getStringForQueuedJob()

    abstract String getStringForJobOnHold()

    abstract String getStringForRunningJob()

    abstract String getJobIdVariable()

    abstract String getJobNameVariable()

    abstract String getQueueVariable()

    abstract String getNodeFileVariable()

    abstract String getSubmitHostVariable()

    abstract String getSubmitDirectoryVariable()

    void setUserEmail(String userEmail) {
        this.userEmail = userEmail
    }

    String getUserEmail() {
        return userEmail
    }

    void setUserMask(String userMask) {
        this.userMask = userMask
    }

    String getUserMask() {
        return userMask
    }

    void setUserGroup(String userGroup) {
        this.userGroup = userGroup
    }

    String getUserGroup() {
        return userGroup
    }

    void setUserAccount(String userAccount) {
        this.userAccount = userAccount
    }

    String getUserAccount() {
        return userAccount
    }


    boolean executesWithoutJobSystem() {
        return false
    }

    abstract String parseJobID(String commandOutput)

    abstract String getSubmissionCommand()

    abstract JobState parseJobState(String stateString)


    List<String> getEnvironmentVariableGlobs() {
        return Collections.unmodifiableList([])
    }

}
