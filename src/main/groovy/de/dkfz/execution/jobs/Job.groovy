/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.execution.jobs

import de.dkfz.config.AppConfig

import de.dkfz.config.ExecutionService
import de.dkfz.eilslabs.tools.logging.IOHelperMethods
import de.dkfz.eilslabs.tools.logging.LoggerWrapper

import java.util.concurrent.atomic.AtomicLong


@groovy.transform.CompileStatic
public class Job {

    public static final String ERR_MSG_ONLY_ONE_JOB_ALLOWED = "A job object is not allowed to run several times.";
    public static final String ENV_LINESEPARATOR = System.getProperty("line.separator");

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(Job.class.getSimpleName());

    private JobType jobType = JobType.STANDARD;

    private final List<Job> arrayChildJobs = new LinkedList<>();

    private static AtomicLong absoluteJobCreationCounter = new AtomicLong();

    /**
     * The name of the job which should be passed to the execution system.
     */
    public final String jobName;

    /**
     * An internal job creation count. Has nothing to do with e.g. PBS / cluster / process id's!
     */
    public final long jobCreationCounter = absoluteJobCreationCounter.incrementAndGet();

    /**
     * The tool you want to call.
     */
    public String toolID;

    //public String toolMD5;

    private boolean isDirty;

    /**
     * Parameters for the tool you want to call
     */
    private final Map<String, String> parameters;

    /**
     * Keeps a list of all unchanged, initial parameters, including default job parameters.
     */
    private final Map<String, Object> allRawInputParameters;
    /**
     * If you want to generated arrays use this. <p>You can do things like: n-m,
     */
    public final List<String> arrayIndices;
    /**
     * Provide a list of files if you want to generate job dependencies.
     */
    public transient final List<Job> parentFiles;

    private Map<String, Object> initialInputParameters = [:]
    /**
     * You should provide i.e. job ids of qsub jobs to automatically create job
     * dependencies.
     */
    public final List<JobDependencyID> dependencyIDs = new LinkedList<JobDependencyID>();

    /**
     * Temporary value which defines the jobs jobState.
     */
    protected transient JobState currentJobState;

    private List<ProcessingCommands> processingCommand = new LinkedList<ProcessingCommands>();

    /**
     * Command object of last execution.
     */
    private transient Command lastCommand;

    /**
     * Stores the result when the job was executed.
     */
    private JobResult runResult;

    /**
     *
     * @param jobName
     * @param defaultParameters
     * @param arrayIndices
     * @param inputParameters
     * @param parentFiles
     */
    public Job(String jobName, Map<String, String> defaultParameters, List<String> arrayIndices, Map<String, Object> inputParameters, List<Job> parentFiles) {
        this.jobName = jobName;
        this.currentJobState = JobState.UNKNOWN;
        this.toolID = toolID;
        this.parameters = [:];

        if (defaultParameters != null)
            parameters.putAll(defaultParameters);

        if (inputParameters != null)
            initialInputParameters.putAll(inputParameters);

        this.arrayIndices = arrayIndices ?: new LinkedList<String>();
        this.parentFiles = parentFiles ?: new LinkedList<Job>();
        if (parentFiles != null) {
            for (Job pf : parentFiles) {
                //if(pf.isSourceFile() && pf.getCreatingJobsResult() == null) continue;
                try {
                    dependencyIDs.addAll(pf.dependencyIDs)
                } catch (Exception ex) {
                    logger.severe("Something is wrong for file: " + pf);
                    logger.postSometimesInfo(ex.message)
                    logger.postSometimesInfo(IOHelperMethods.getStackTraceAsString(ex))
                }
            }
        }
       // this.filesToVerify = filesToVerify ?: new LinkedList<BaseFile>();
        if (arrayIndices == null)
            return;
    }


    //private abstract Map<String, String> convertParameterObject(String k, Object _v)


    //private abstract File replaceParametersInFilePath(String bf, Map<String, Object> parameters)


    private void setJobType(JobType jobType) {
        this.jobType = jobType;
    }

    /**
     * Run job
     * @param executionService
     * @param configuration
     * @param parameters
     * @return
     */
    public JobResult run(ExecutionService executionService, AppConfig configuration, Map<String, String> parameters) {
        if (runResult != null)
            throw new RuntimeException(ERR_MSG_ONLY_ONE_JOB_ALLOWED);

        StringBuilder dbgMessage = new StringBuilder();
        StringBuilder jobDetailsLine = new StringBuilder();
        Command cmd;
        boolean isArrayJob = arrayIndices;// (arrayIndices != null && arrayIndices.size() > 0);
        boolean runJob = true;

        //Remove duplicate job ids as qsub cannot handle duplicate keys => job will hold forever as it releases the dependency queue linearly
        List<String> dependencies = dependencyIDs.collect { JobDependencyID jobDependencyID -> return jobDependencyID.getId(); }.unique() as List<String>;
        this.parameters.putAll(parameters);

        appendProcessingCommands(configuration)

        cmd = executeJob(executionService, dependencies, dbgMessage)
            jobDetailsLine << " => " + cmd.getExecutionID()
            //System.out.println(jobDetailsLine.toString())
            if (cmd.getExecutionID() == null) {
            // TODO:  context.addErrorEntry(ExecutionContextError.EXECUTION_SUBMISSION_FAILURE.expand("Please check your submission command manually.\n\t  Is your access group set properly? [${context.getAnalysis().getUsergroup()}]\n\t  Can the submission binary handle your binary?\n\t  Is your submission system offline?"));
//TODO:                if (Roddy.getFeatureToggleValue(AvailableFeatureToggles.BreakSubmissionOnError)) {
//                    context.abortJobSubmission();
//                }
            }

        runResult = new JobResult(cmd, cmd.getExecutionID(), runJob, isArrayJob,null, parameters, parentFiles);
        //For auto filenames. Get the job id and push propagate it to all filenames.

        if (isArrayJob) {
            postProcessArrayJob(runResult)
        } else {
            JobManager.getInstance().addJobStatusChangeListener(this);
        }
        lastCommand = cmd;
        return runResult;
    }

    private void appendProcessingCommands(AppConfig configuration) {
// Only extract commands from file if none are set
        if (getListOfProcessingCommand().size() == 0) {
            //File srcTool = configuration.getProperty("sourceToolPath");

            //Look in the configuration for resource options
            ProcessingCommands extractedPCommands = JobManager.getInstance().getProcessingCommandsFromConfiguration(configuration, toolID);

            //Look in the script if no options are configured
            if (extractedPCommands == null)
                extractedPCommands = JobManager.getInstance().extractProcessingCommandsFromToolScript((File) configuration.getProperty("srcTool"));

            if (extractedPCommands != null)
                this.addProcessingCommand(extractedPCommands);
        }
    }

    //private abstract JobResult handleDifferentJobRun(StringBuilder dbgMessage)

    /**
     * Checks, if a job needs to be rerun.
     * @param dbgMessage
     * @return
     */
    //private abstract boolean checkIfJobShouldRerun(StringBuilder dbgMessage)

    //private abstract List verifyFiles(StringBuilder dbgMessage)



    private void postProcessArrayJob(JobResult runResult) {
        Map<String, Object> prmsAsStringMap = new LinkedHashMap<>();
        for (String k : parameters.keySet()) {
            prmsAsStringMap.put(k, parameters.get(k));
        }
        jobType = JobType.ARRAY_HEAD;
        //TODO Think of proper array index handling!
        int i = 1;
        for (String arrayIndex : arrayIndices) {
            JobResult jr = JobManager.getInstance().convertToArrayResult(this, runResult, i++);

            Job childJob = new Job(jobName + "[" + arrayIndex + "]", null, null,prmsAsStringMap, parentFiles);
            childJob.setJobType(JobType.ARRAY_CHILD);
            childJob.setRunResult(jr);
            arrayChildJobs.add(childJob);
            JobManager.getInstance().addJobStatusChangeListener(childJob);
           // this.jobmanager.addExecutedJob(childJob);
        }
    }

    /**
     * Finally execute a job.
     * @param dependencies
     * @param dbgMessage
     * @param cmd
     * @return
     */
    private Command executeJob(ExecutionService executionService, List<String> dependencies, StringBuilder dbgMessage) {
        //File tool = context.getConfiguration().getProcessingToolPath(context, toolID);
        setJobState(JobState.UNSTARTED);
        Command cmd = JobManager.getInstance().createCommand(this, null,dependencies);
        executionService.execute(cmd);
        if (LoggerWrapper.isVerbosityMedium()) {
            dbgMessage << ENV_LINESEPARATOR << "\tcommand was created and executed for job. ID is " + cmd.getExecutionID() << ENV_LINESEPARATOR;
        }
        if (LoggerWrapper.isVerbosityHigh()) logger.info(dbgMessage.toString());
        return cmd;
    }

    public void addProcessingCommand(ProcessingCommands processingCommand) {
        if (processingCommand == null) return;
        this.processingCommand.add(processingCommand);
    }

    public JobType getJobType() {
        return jobType;
    }

    public List<ProcessingCommands> getListOfProcessingCommand() {
        return processingCommand;
    }


    public List<String> getParentFiles() {
        if (parentFiles != null)
            return new LinkedList<>(parentFiles);
        else
            return new LinkedList<>();
    }

    public List<Job> getParentJobs() {
        return dependencyIDs.collect { JobDependencyID jid -> jid?.job }.findAll { Job job -> job != null } as List<Job>
    }



    public JobResult getRunResult() {
        return runResult;
    }

    public void setRunResult(JobResult result) {
        this.runResult = result;
    }

    /**
     * If the job was executed this return the jobs id otherwise null.
     *
     * @return
     */
    public String getJobID() {
        if (runResult != null)
            if (runResult.getJobID() != null)
                return runResult.getJobID().getShortID();
            else
                return "Unknown";
        else
            return null;
    }


    private File _logFile = null;

    public String getJobName() {
        return jobName;
    }

    public void setJobState(JobState js) {
        if (jobType == JobType.ARRAY_HEAD)
            return;
        JobState old = this.currentJobState;
        this.currentJobState = js;
    }

    /**
     * Return job state
     * @return
     */
    public JobState getJobState() {
        if (jobType == JobType.ARRAY_HEAD) {
            int runningJobs = 0;
            int failedJobs = 0;
            int finishedJobs = 0;
            int unknownJobs = 0;
            for (Job job : arrayChildJobs) {
                if (job.getJobState().isPlannedOrRunning())
                    runningJobs++;
                else if (job.getJobState() == JobState.FAILED)
                    failedJobs++;
                else if (job.getJobState() == JobState.OK)
                    finishedJobs++;
                else if (job.getJobState() == JobState.UNKNOWN)
                    unknownJobs++;
            }
            if (failedJobs > 0) return JobState.FAILED;
            if (unknownJobs > 0) return JobState.UNKNOWN;
            if (runningJobs > 0) return JobState.RUNNING;
            return JobState.OK;
        }
        return currentJobState != null ? currentJobState : JobState.UNKNOWN;
    }

    public Command getLastCommand() {
        return lastCommand;
    }



    public Map<String, String> getParameters() {
        return parameters;
    }

}
