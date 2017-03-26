/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs;

import de.dkfz.roddy.execution.jobs.JobResult as JobResult
import de.dkfz.eilslabs.batcheuphoria.config.ResourceSet;
import de.dkfz.eilslabs.batcheuphoria.execution.ExecutionService
import de.dkfz.roddy.tools.LoggerWrapper
import groovy.transform.CompileStatic

/**
 * Basic factory and manager class for Job and Command management
 * Currently supported are qsub via PBS or SGE. Other cluster systems or custom job submission / execution system are possible.
 *
 * @author michael
 */
@CompileStatic
public abstract class JobManager<C extends Command> {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(JobManager.class.getSimpleName());

    public static final int JOBMANAGER_DEFAULT_UPDATEINTERVAL = 300
    public static final boolean JOBMANAGER_DEFAULT_CREATE_DAEMON = true;
    public static final boolean JOBMANAGER_DEFAULT_TRACKUSERJOBSONLY = false
    public static final boolean JOBMANAGER_DEFAULT_TRACKSTARTEDJOBSONLY = false

    public static final String BE_DEFAULT_JOBID = "BE_JOBID"

    public static final String BE_DEFAULT_JOBARRAYINDEX = "BE JOBARRAYINDEX"

    public static final String BE_DEFAULT_JOBSCRATCH = "BE JOBSCRATCH"

    protected String jobIDIdentifier = BE_DEFAULT_JOBID;

    protected String jobArrayIndexIdentifier = BE_DEFAULT_JOBARRAYINDEX;

    protected String jobScratchIdentifier = BE_DEFAULT_JOBSCRATCH;

    protected final ExecutionService executionService;

    protected Thread updateDaemonThread;

    protected boolean closeThread;

    protected List<C> listOfCreatedCommands = new LinkedList<>();

    protected boolean isTrackingOfUserJobsEnabled;

    protected boolean queryOnlyStartedJobs;

    protected String userIDForQueries;

    private String userEmail;

    private String userMask;

    private String userGroup;

    private String userAccount;

    private boolean isParameterFileEnabled;

    public JobManager(ExecutionService executionService, JobManagerCreationParameters parms) {
        this.executionService = executionService;

        this.isTrackingOfUserJobsEnabled = parms.trackUserJobsOnly
        this.queryOnlyStartedJobs = parms.trackOnlyStartedJobs
        this.userIDForQueries = parms.userIdForJobQueries
        if (!userIDForQueries && isTrackingOfUserJobsEnabled) {
            logger.warning("Silently falling back to default job tracking behaviour. The user name was not set properly and the system cannot track the users jobs.")
            isTrackingOfUserJobsEnabled = false
        }
        this.jobIDIdentifier = parms.jobIDIdentifier
        this.jobScratchIdentifier = parms.jobScratchIdentifier
        this.jobArrayIndexIdentifier = parms.jobArrayIDIdentifier

        //Create a daemon thread which automatically calls queryJobStatus from time to time...
        try {
            if (parms.createDaemon) {
                int interval = parms.updateInterval
                createUpdateDaemonThread(interval)
            }
        } catch (Exception ex) {
            logger.severe("Creating the command factory daemon failed for some reason. Roddy will not be able to query the job system.", ex);
        }
    }

    public void createUpdateDaemonThread(int interval) {

        if (updateDaemonThread != null) {
            closeThread = true;
            try {
                updateDaemonThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            updateDaemonThread = null;
        }

        updateDaemonThread = Thread.startDaemon("Command factory update daemon.", {
            while (!closeThread) {
                try {
                    updateJobStatus();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(interval * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        })
    }

    public abstract C createCommand(GenericJobInfo jobInfo);

    public abstract C createCommand(Job job, String jobName, List<ProcessingCommands> processingCommands, File tool, Map<String, String> parameters, List<String> dependencies, List<String> arraySettings);

    public C createCommand(Job job, File tool, List<String> dependencies) {
        C c = (C) createCommand(job, job.jobName, job.getListOfProcessingCommand(), tool, job.getParameters(), dependencies, job.arrayIndices);
        c.setJob(job);
        return c;
    }
//
//    public Command.DummyCommand createDummyCommand(Job job, ExecutionContext run, String jobName, List<String> arraySettings) {
//        return new Command.DummyCommand(job, run, jobName, arraySettings != null && arraySettings.size() > 0);
//    }

    public void setTrackingOfUserJobsEnabled(boolean trackingOfUserJobsEnabled) {
        isTrackingOfUserJobsEnabled = trackingOfUserJobsEnabled;
    }

    public void setQueryOnlyStartedJobs(boolean queryOnlyStartedJobs) {
        this.queryOnlyStartedJobs = queryOnlyStartedJobs;
    }

    public void setUserIDForQueries(String userIDForQueries) {
        this.userIDForQueries = userIDForQueries;
    }

    /**
     * Shortcut to runJob with runDummy = false
     *
     * @param job
     * @return
     */
    public JobResult runJob(Job job) {
        return runJob(job, false);
    }

    public abstract JobResult runJob(Job job, boolean runDummy);

    void startHeldJobs(List<Job> jobs) {}

    boolean isHoldJobsEnabled() { return false }

    public abstract de.dkfz.roddy.execution.jobs.JobDependencyID createJobDependencyID(Job job, String jobResult);

    public abstract ProcessingCommands convertResourceSet(ResourceSet resourceSet);

    public abstract ProcessingCommands parseProcessingCommands(String alignmentProcessingOptions);

//    public abstract ProcessingCommands getProcessingCommandsFromConfiguration(Configuration configuration, String toolID);

    public abstract ProcessingCommands extractProcessingCommandsFromToolScript(File file);

    public List<C> getListOfCreatedCommands() {
        List<C> newList = new LinkedList<>();
        synchronized (listOfCreatedCommands) {
            newList.addAll(listOfCreatedCommands);
        }
        return newList;
    }

    /**
     * Tries to reverse assemble job information out of an executed command.
     * The format should be [id], [command, i.e. qsub...]
     *
     * @param commandString
     * @return
     */
    public abstract Job parseToJob(String commandString);

    public abstract GenericJobInfo parseGenericJobInfo(String command);

    public abstract JobResult convertToArrayResult(Job arrayChildJob, JobResult parentJobsResult, int arrayIndex);

    public abstract void updateJobStatus();

    /**
     * Queries the status of all jobs in the list.
     *
     * @param jobIDs
     * @return
     */
    public abstract Map<String, JobState> queryJobStatus(List<String> jobIDs);

    public abstract void queryJobAbortion(List<Job> executedJobs);

    public abstract void addJobStatusChangeListener(Job job);

    public abstract String getLogFileWildcard(Job job);

    public abstract boolean compareJobIDs(String jobID, String id);

    public void addCommandToList(C pbsCommand) {
        synchronized (listOfCreatedCommands) {
            listOfCreatedCommands.add(pbsCommand);
        }
    }

    public int waitForJobsToFinish() {
        return 0;
    }

    public abstract String getStringForQueuedJob();

    public abstract String getStringForJobOnHold();

    public abstract String getStringForRunningJob();

    public String getJobIDIdentifier() {
        return jobIDIdentifier;
    }

    public void setJobIDIdentifier(String jobIDIdentifier) {
        this.jobIDIdentifier = jobIDIdentifier;
    }

    public String getJobArrayIndexIdentifier() {
        return jobArrayIndexIdentifier;
    }

    public void setJobArrayIndexIdentifier(String jobArrayIndexIdentifier) {
        this.jobArrayIndexIdentifier = jobArrayIndexIdentifier;
    }

    public String getJobScratchIdentifier() {
        return jobScratchIdentifier;
    }

    public void setJobScratchIdentifier(String jobScratchIdentifier) {
        this.jobScratchIdentifier = jobScratchIdentifier;
    }

    public abstract String getSpecificJobIDIdentifier();

    public abstract String getSpecificJobArrayIndexIdentifier();

    public abstract String getSpecificJobScratchIdentifier();

    public Map<String, String> getSpecificEnvironmentSettings() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put(jobIDIdentifier, getSpecificJobIDIdentifier());
        map.put(jobArrayIndexIdentifier, getSpecificJobArrayIndexIdentifier());
        map.put(jobScratchIdentifier, getSpecificJobScratchIdentifier());
        return map;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserMask(String userMask) {
        this.userMask = userMask;
    }

    public String getUserMask() {
        return userMask;
    }

    public void setUserGroup(String userGroup) {
        this.userGroup = userGroup;
    }

    public String getUserGroup() {
        return userGroup;
    }

    public void setUserAccount(String userAccount) {
        this.userAccount = userAccount;
    }

    public String getUserAccount() {
        return userAccount;
    }

    public void setParameterFileEnabled(boolean parameterFileEnabled) {
        isParameterFileEnabled = parameterFileEnabled;
    }

    public boolean isParameterFileEnabled() {
        return isParameterFileEnabled;
    }

    /**
     * Tries to get the log for a running job.
     * Returns an empty array, if the job's jobState is not RUNNING
     *
     * @param job
     * @return
     */
    public abstract String[] peekLogFile(Job job);

    /**
     * Stores a new job jobState info to an execution contexts job jobState log file.
     *
     * @param job
     */
    public String getJobStateInfoLine(Job job) {
        String millis = "" + System.currentTimeMillis();
        millis = millis.substring(0, millis.length() - 3);
        String code = "255";
        if (job.getJobState() == JobState.UNSTARTED)
            code = "N";
        else if (job.getJobState() == JobState.ABORTED)
            code = "A";
        else if (job.getJobState() == JobState.OK)
            code = "C";
        else if (job.getJobState() == JobState.FAILED)
            code = "E";
        if (null != job.getJobID())
            return String.format("%s:%s:%s", job.getJobID(), code, millis);

        logger.postSometimesInfo("Did not store info for job " + job.getJobName() + ", job id was null.");
        return null;
    }

    public String getLogFileName(Job p) {
        return p.getJobName() + ".o" + p.getJobID();
    }

    public String getLogFileName(Command command) {
        return command.getJob().getJobName() + ".o" + command.getExecutionID().getId();
    }

    public boolean executesWithoutJobSystem() {
        return false;
    }

    public abstract String parseJobID(String commandOutput);

    public abstract String getSubmissionCommand();
}
