/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.execution.jobs;

import de.dkfz.config.AppConfig;
import de.dkfz.config.ExecutionService;
import de.dkfz.config.ResourceSet;
import de.dkfz.eilslabs.tools.logging.LoggerWrapper;

import java.io.File;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Basic factory and manager class for Job and Command management
 * Currently supported are qsub via PBS or SGE. Other cluster systems or custom job submission / de.dkfz.execution system are possible.
 *
 * @author michael
 */
public abstract class JobManager<C extends Command> {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(JobManager.class.getSimpleName());
    private static JobManager jobManager;


    protected Thread updateDaemonThread;

    protected boolean closeThread;
    protected List<C> listOfCreatedCommands = new LinkedList<>();

    public JobManager() {
    }

    public static void initializeFactory(JobManager factory) {
        jobManager = factory;
    }

    public static String createJobName(String toolName, boolean reduceLevel) {
        return createJobName(toolName, reduceLevel, "","");
    }

    public static String createJobName(String toolName, boolean reduceLevel, String pid, String runtime) {
        //ExecutionContext rp = bf.getExecutionContext();
        //String runtime = rp.getTimestampString();
        StringBuilder sb = new StringBuilder();
        sb.append("r").append(runtime).append("_").append(pid).append("_").append(toolName);
        return sb.toString();
    }

    public static JobManager getInstance() {

        return jobManager;
    }

    public abstract void createUpdateDaemonThread(int interval);

    public abstract C createCommand(GenericJobInfo jobInfo);

    public abstract C createCommand(Job job, String jobName, List<ProcessingCommands> processingCommands, File tool, Map<String, String> parameters, List<String> dependencies, List<String> arraySettings);

    public C createCommand(Job job, File tool, List<String> dependencies) {
        C c = createCommand(job, job.jobName, job.getListOfProcessingCommand(), tool, job.getParameters(), dependencies, job.arrayIndices);
        c.setJob(job);
        return c;
    }
/*
    public Command.DummyCommand createDummyCommand(Job job, String jobName, List<String> arraySettings) {
        return new Command.DummyCommand(job,jobName, arraySettings != null && arraySettings.size() > 0);
    }
*/
    public abstract JobDependencyID createJobDependencyID(Job job, String jobResult);

    public abstract ProcessingCommands convertResourceSet(AppConfig configuration, ResourceSet resourceSet);

    public abstract ProcessingCommands parseProcessingCommands(String alignmentProcessingOptions);

    public abstract ProcessingCommands getProcessingCommandsFromConfiguration(AppConfig configuration, String toolID);

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

    /**
     * Queries the status of all jobs in the list.
     *
     * @param jobIDs
     * @return
     */
    public abstract Map<String, JobState> queryJobStatus(List<String> jobIDs);

    public abstract void queryJobAbortion(List<Job> executedJobs,  ExecutionService executionService);

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

    public void addSpecificSettingsToConfiguration(AppConfig configuration) {

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
     * Stores a new job jobState info to an de.dkfz.execution contexts job jobState log file.
     *
     * @param job
     */
    /**
     * Stores a new job jobState info to an execution contexts job jobState log file.
     *
     * @param job
     */
    public abstract void storeJobStateInfo(Job job);

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
