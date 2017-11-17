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

/**
 * Basic factory and manager class for BEJob and Command management
 * Currently supported are qsub via PBS or SGE. Other cluster systems or custom job submission / execution system are possible.
 *
 * @author michael
 */
@CompileStatic
abstract class BatchEuphoriaJobManager<C extends Command> {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(BatchEuphoriaJobManager.class.getSimpleName())

    public static final int JOBMANAGER_DEFAULT_UPDATEINTERVAL = 300
    public static final boolean JOBMANAGER_DEFAULT_CREATE_DAEMON = true
    public static final boolean JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY = false
    public static final boolean JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY = false

    protected final BEExecutionService executionService

    protected Thread updateDaemonThread

    protected boolean closeThread

    protected List<C> listOfCreatedCommands = new LinkedList<>()

    protected boolean isTrackingOfUserJobsEnabled

    protected boolean queryOnlyStartedJobs

    protected String userIDForQueries

    protected boolean strictMode

    private String userEmail

    private String userMask

    private String userGroup

    private String userAccount

    private Boolean isHoldJobsEnabled = null

    private File defaultLoggingDirectory

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

        //Create a daemon thread which automatically calls queryJobStatus from time to time...
        try {
            if (parms.createDaemon) {
                int interval = parms.updateInterval
                createUpdateDaemonThread(interval)
            }
        } catch (Exception ex) {
            logger.severe("Creating the command factory daemon failed for some reason. BE will not be able to query the job system.", ex)
        }
    }

    void createUpdateDaemonThread(int interval) {

        if (updateDaemonThread != null) {
            closeThread = true
            try {
                updateDaemonThread.join()
            } catch (InterruptedException e) {
                e.printStackTrace()
            }
            updateDaemonThread = null
        }

        updateDaemonThread = Thread.startDaemon("Command factory update daemon.", {
            while (!closeThread) {
                updateJobStatus()
                try {
                    Thread.sleep(interval * 1000)
                } catch (InterruptedException e) {
                    e.printStackTrace()
                }
            }
        })
    }

    abstract C createCommand(BEJob job, String jobName, List<ProcessingParameters> processingParameters, File tool, Map<String, String> parameters, List<String> parentJobs)

    C createCommand(BEJob job, File tool, List<String> parentJobs, Map<String, String> parameters) {
        C c = (C) createCommand(job, job.jobName, job.getListOfProcessingParameters(), tool, parameters, parentJobs)
        c.setJob(job)
        return c
    }

    void setTrackingOfUserJobsEnabled(boolean trackingOfUserJobsEnabled) {
        isTrackingOfUserJobsEnabled = trackingOfUserJobsEnabled
    }

    void setQueryOnlyStartedJobs(boolean queryOnlyStartedJobs) {
        this.queryOnlyStartedJobs = queryOnlyStartedJobs
    }

    void setUserIDForQueries(String userIDForQueries) {
        this.userIDForQueries = userIDForQueries
    }

    /**
     * Shortcut to runJob with runDummy = false
     *
     * @param job
     * @return
     */
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
            jobResult = new BEJobResult(command, job, res, false, job.tool, job.parameters, job.parentJobs as List<BEJob>)
            job.setRunResult(jobResult)
        } else {
            def job = command.getJob()
            jobResult = new BEJobResult(command, job, res, false, job.tool, job.parameters, job.parentJobs as List<BEJob>)
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

    List<C> getListOfCreatedCommands() {
        List<C> newList = new LinkedList<>()
        synchronized (listOfCreatedCommands) {
            newList.addAll(listOfCreatedCommands)
        }
        return newList
    }

    /**
     * Tries to reverse assemble job information out of an executed command.
     * The format should be [id], [command, i.e. qsub...]
     *
     * @param commandString
     * @return
     */
    abstract BEJob parseToJob(String commandString)

    abstract GenericJobInfo parseGenericJobInfo(String command)

    abstract BEJobResult convertToArrayResult(BEJob arrayChildJob, BEJobResult parentJobsResult, int arrayIndex)

    abstract void updateJobStatus()

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
    abstract Map<BEJob, JobState> queryJobStatus(List<BEJob> jobs, boolean forceUpdate = false)

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
    abstract Map<String, JobState> queryJobStatusById(List<BEJobID> jobIds, boolean forceUpdate = false)

    /**
     * Queries the status of all jobs.
     *
     * @return
     */
    abstract Map<String, JobState> queryJobStatusAll(boolean forceUpdate = false)

    /**
     * Will be used to gather extended information about a job like:
     * - The used memory
     * - The used cores
     * - The used walltime
     *
     * @param jobs
     * @param forceUpdate
     * @return
     */
    abstract Map<String, BEJob> queryExtendedJobState(List<BEJob> jobs, boolean forceUpdate)

    /**
     * Will be used to gather extended information about a job like:
     * - The used memory
     * - The used cores
     * - The used walltime
     *
     * @param jobIds
     * @param forceUpdate
     * @return
     */
    abstract Map<String, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds, boolean forceUpdate)

    /**
     * Try to abort a range of jobs
     * @param executedJobs
     */
    abstract void queryJobAbortion(List<BEJob> executedJobs)

    abstract void addJobStatusChangeListener(BEJob job)

    abstract String getLogFileWildcard(BEJob job)

    void addCommandToList(C pbsCommand) {
        synchronized (listOfCreatedCommands) {
            listOfCreatedCommands.add(pbsCommand)
        }
    }

    int waitForJobsToFinish() {
        return 0
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


    // ???
    File getDefaultLoggingDirectory() {
        return defaultLoggingDirectory
    }

    void setDefaultLoggingDirectory(File file) {
        this.defaultLoggingDirectory = file
    }

    abstract JobState parseJobState(String stateString)


    List<String> getEnvironmentVariableGlobs() {
        return Collections.unmodifiableList([])
    }

}
