/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.tools.LoggerWrapper
import groovy.transform.CompileStatic
import sun.reflect.generics.reflectiveObjects.NotImplementedException

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
            logger.severe("Creating the command factory daemon failed for some reason. Roddy will not be able to query the job system.", ex)
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
                try {
                    updateJobStatus()
                } catch (Exception e) {
                    e.printStackTrace()
                }
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
            def jobID = new BEJobID(exID)
            command.setExecutionID(jobID)
            job.resetJobID(exID)
            jobResult = new BEJobResult(command, job, res, false, job.tool, job.parameters, job.parentJobs as List<BEJob>)
            job.setRunResult(jobResult)
        } else {
            def job = command.getJob()
            jobResult = new BEJobResult(command, job, res, false, job.tool, job.parameters, job.parentJobs as List<BEJob>)
            job.setRunResult(jobResult)
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
    abstract Map<String, JobState> queryJobStatusById(List<String> jobIds, boolean forceUpdate = false)

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
    abstract Map<String, GenericJobInfo> queryExtendedJobStateById(List<String> jobIds, boolean forceUpdate)

    /**
     * Try to abort a range of jobs
     * @param executedJobs
     */
    abstract void queryJobAbortion(List<BEJob> executedJobs)

    abstract void addJobStatusChangeListener(BEJob job)

    abstract String getLogFileWildcard(BEJob job)

    abstract boolean compareJobIDs(String jobID, String id)

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

    String getJobIDIdentifier() {
        return jobIDIdentifier
    }

    void setJobIDIdentifier(String jobIDIdentifier) {
        this.jobIDIdentifier = jobIDIdentifier
    }

    String getJobArrayIndexIdentifier() {
        return jobArrayIndexIdentifier
    }

    void setJobArrayIndexIdentifier(String jobArrayIndexIdentifier) {
        this.jobArrayIndexIdentifier = jobArrayIndexIdentifier
    }

    String getJobScratchIdentifier() {
        return jobScratchIdentifier
    }

    void setJobScratchIdentifier(String jobScratchIdentifier) {
        this.jobScratchIdentifier = jobScratchIdentifier
    }

    abstract String getJobIdVariable()

    abstract String getJobArrayIndexVariable()

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

    /**
     * Tries to get the log for a running job.
     * Returns an empty array, if the job's jobState is not RUNNING
     *
     * @param job
     * @return
     */
    abstract String[] peekLogFile(BEJob job)

    String getLogFileName(BEJob p) {
        return p.getJobName() + ".o" + p.getJobID()
    }

    String getLogFileName(Command command) {
        return command.getJob().getJobName() + ".o" + command.getExecutionID().getId()
    }

    boolean executesWithoutJobSystem() {
        return false
    }

    abstract String parseJobID(String commandOutput)

    abstract String getSubmissionCommand()

    File getDefaultLoggingDirectory() {
        logger.severe("We do not know yet, how to query the default logging directory... the submission server does not necessarily have to know about this.")
        logger.severe("We assume, that the logging directory is set to the current working directory automatically or to the home folder.")
        return executionService.queryWorkingDirectory()
    }


    abstract JobState parseJobState(String stateString)


    List<String> getEnvironmentVariableGlobs() {
        return Collections.unmodifiableList([])
    }

}
