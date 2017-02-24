/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs

import java.util.concurrent.atomic.AtomicLong

/**
 * The job class represents a generic and abstract form of cluster job which can be run using a job manager.
 * When a job is executed with the JM, the used Command and a JobResult object will be created and added to this
 * object.
 */
@groovy.transform.CompileStatic
class Job {


    public static class FakeJob extends Job {
        public FakeJob() {
            super("Fakejob", null, null, [], [:], []);
        }

//        public FakeJob(ExecutionContext context) {
//            super(context, "Fakejob", null, null);
//        }
    }


    private static final de.dkfz.roddy.tools.LoggerWrapper logger = de.dkfz.roddy.tools.LoggerWrapper.getLogger(Job.class.getSimpleName())

    private JobType jobType = JobType.STANDARD

    private final List<Job> arrayChildJobs = new LinkedList<>()

    private static AtomicLong absoluteJobCreationCounter = new AtomicLong()

    /**
     * The name of the job which should be passed to the execution system.
     */
    public final String jobName

    /**
     * An internal job creation count. Has nothing to do with e.g. PBS / cluster / process id's!
     */
    public final long jobCreationCounter = absoluteJobCreationCounter.incrementAndGet()

    /**
     * The tool you want to call.
     */
    protected File tool

    /**
     * MD5 sum of the called tool
     */
    protected String toolMD5

    /**
     * Parameters for the tool you want to call
     */
    protected final Map<String, String> parameters

    /**
     * If you want to generated arrays use this. <p>You can do things like: n-m,
     */
    protected final List<String> arrayIndices

    protected final List<Job> parentJobs

    /**
     * You should provide i.e. job ids of qsub jobs to automatically create job
     * dependencies.
     */
    public final List<JobDependencyID> dependencyIDs = new LinkedList<JobDependencyID>()

    /**
     * Temporary value which defines the jobs jobState.
     */
    protected transient JobState currentJobState

    private List<ProcessingCommands> processingCommand = new LinkedList<ProcessingCommands>()

    /**
     * Command object of last execution.
     */
    private transient Command lastCommand

    /**
     * Stores the result when the job was executed.
     */
    private JobResult runResult

    Job(String jobName, File tool, String toolMD5, List<String> arrayIndices, Map<String, String> parameters, List<Job> parentJobs) {
        this.jobName = jobName
        this.currentJobState = JobState.UNKNOWN
        this.tool = tool
        this.toolMD5 = toolMD5
        this.parameters = parameters
        this.parentJobs = parentJobs
        this.arrayIndices = arrayIndices ?: new LinkedList<String>()
    }

    private void setJobType(JobType jobType) {
        this.jobType = jobType
    }

    //TODO Create a runArray method which returns several job results with proper array ids.
//    JobResult run() {
//        null
//        if (runResult != null)
//            throw new RuntimeException(Constants.ERR_MSG_ONLY_ONE_JOB_ALLOWED)
//
//        ExecutionContextLevel contextLevel = context.getExecutionContextLevel()
//        Configuration configuration = context.getConfiguration()
//        File tool = configuration.getProcessingToolPath(context, toolID)
//
//        StringBuilder dbgMessage = new StringBuilder()
//        StringBuilder jobDetailsLine = new StringBuilder()
//        Command cmd
//        boolean isArrayJob = arrayIndices// (arrayIndices != null && arrayIndices.size() > 0);
//        boolean runJob
//
//        //Remove duplicate job ids as qsub cannot handle duplicate keys => job will hold forever as it releases the dependency queue linearly
//        List<String> dependencies = dependencyIDs.collect { JobDependencyID jobDependencyID -> return jobDependencyID.getId() }.unique() as List<String>
//        this.parameters.putAll(convertParameterObject(Constants.RODDY_PARENT_JOBS, dependencies))
//
//        appendProcessingCommands(configuration)
//
//        //See if the job should be executed
//        if (contextLevel == ExecutionContextLevel.RUN || contextLevel == ExecutionContextLevel.CLEANUP) {
//            runJob = true //The job is always executed if run is selected
//            jobDetailsLine << "  Running job " + jobName
//        } else if (contextLevel == ExecutionContextLevel.RERUN || contextLevel == ExecutionContextLevel.TESTRERUN) {
//            runJob = checkIfJobShouldRerun(dbgMessage)
//            jobDetailsLine << "  Rerun job " + jobName
//        } else {
//            return handleDifferentJobRun(dbgMessage)
//        }
//
//        //Execute the job or create a dummy command.
//        if (runJob) {
//            cmd = executeJob(dependencies, dbgMessage)
//            jobDetailsLine << " => " + cmd.getExecutionID()
//            System.out.println(jobDetailsLine.toString())
//            if (cmd.getExecutionID() == null) {
//                context.addErrorEntry(ExecutionContextError.EXECUTION_SUBMISSION_FAILURE.expand("Please check your submission command manually.\n\t  Is your access group set properly? [${context.getAnalysis().getUsergroup()}]\n\t  Can the submission binary handle your binary?\n\t  Is your submission system offline?"))
//                if (Roddy.getFeatureToggleValue(AvailableFeatureToggles.BreakSubmissionOnError)) {
//                    context.abortJobSubmission()
//                }
//            }
//        } else {
//            cmd = JobManager.getInstance().createDummyCommand(this, context, jobName, arrayIndices)
//            this.setJobState(JobState.DUMMY)
//        }
//
//        runResult = new JobResult(context, cmd, cmd.getExecutionID(), runJob, isArrayJob, tool, parameters, parentJobs)
//        //For auto filenames. Get the job id and push propagate it to all filenames.
//
//        if (runResult?.jobID?.shortID) {
//            allRawInputParameters.each { String k, Object o ->
//                BaseFile bf = o instanceof BaseFile ? (BaseFile) o : null
//                if (!bf) return
//
//                String absolutePath = bf.getPath().getAbsolutePath()
//                if (absolutePath.contains('${RODDY_JOBID}')) {
//                    bf.setPath(new File(absolutePath.replace('${RODDY_JOBID}', runResult.jobID.shortID)))
//                }
//            }
//        }
//
//        if (isArrayJob) {
//            postProcessArrayJob(runResult)
//        } else {
//            JobManager.getInstance().addJobStatusChangeListener(this)
//        }
//        lastCommand = cmd
//        return runResult
//    }

//    private void postProcessArrayJob(JobResult runResult) {
//        Map<String, Object> prmsAsStringMap = new LinkedHashMap<>()
//        for (String k : parameters.keySet()) {
//            prmsAsStringMap.put(k, parameters.get(k))
//        }
//        jobType = JobType.ARRAY_HEAD
//        //TODO Think of proper array index handling!
//        int i = 1
//        for (String arrayIndex : arrayIndices) {
//            JobResult jr = JobManager.getInstance().convertToArrayResult(this, runResult, i++)
//
//            Job childJob = new Job(context, jobName + "[" + arrayIndex + "]", toolID, prmsAsStringMap, parentJobs, filesToVerify)
//            childJob.setJobType(JobType.ARRAY_CHILD)
//            childJob.setRunResult(jr)
//            arrayChildJobs.add(childJob)
//            JobManager.getInstance().addJobStatusChangeListener(childJob)
//            this.context.addExecutedJob(childJob)
//        }
//    }

//    /**
//     * Finally execute a job.
//     * @param dependencies
//     * @param dbgMessage
//     * @param cmd
//     * @return
//     */
//    private Command executeJob(List<String> dependencies, StringBuilder dbgMessage) {
//        String sep = Constants.ENV_LINESEPARATOR
//        File tool = context.getConfiguration().getProcessingToolPath(context, toolID)
//        setJobState(JobState.UNSTARTED)
//        Command cmd = JobManager.getInstance().createCommand(this, tool, dependencies)
//        ExecutionService.getInstance().execute(cmd)
//        if (LoggerWrapper.isVerbosityMedium()) {
//            dbgMessage << sep << "\tcommand was created and executed for job. ID is " + cmd.getExecutionID() << sep
//        }
//        if (LoggerWrapper.isVerbosityHigh()) logger.info(dbgMessage.toString())
//        return cmd
//    }

    void addProcessingCommand(ProcessingCommands processingCommand) {
        if (processingCommand == null) return
        this.processingCommand.add(processingCommand)
    }

    JobType getJobType() {
        return jobType
    }

    Map<String, String> getParameters() {
        return parameters
    }

    List<ProcessingCommands> getListOfProcessingCommand() {
        return processingCommand
    }

    List<Job> getParentJobs() {
        return parentJobs
    }

    void setRunResult(JobResult result) {
        this.runResult = result
    }

    JobResult getRunResult() {
        return runResult
    }

    /**
     * If the job was executed this return the jobs id otherwise null.
     *
     * @return
     */
    String getJobID() {
        if (runResult != null)
            if (runResult.getJobID() != null)
                return runResult.getJobID().getShortID()
            else
                return "Unknown"
        else
            return null
    }

    String getToolID() {
        return toolID
    }

    File getTool() {
        return tool
    }

    String getToolMD5() {
        return toolMD5
    }

    private File _logFile = null

    String getJobName() {
        return jobName
    }

    void setJobState(JobState js) {
        if (jobType == JobType.ARRAY_HEAD)
            return
        JobState old = this.currentJobState
        this.currentJobState = js
    }

    JobState getJobState() {
        if (jobType == JobType.ARRAY_HEAD) {
            int runningJobs = 0
            int failedJobs = 0
            int finishedJobs = 0
            int unknownJobs = 0
            for (Job job : arrayChildJobs) {
                if (job.getJobState().isPlannedOrRunning())
                    runningJobs++
                else if (job.getJobState() == JobState.FAILED)
                    failedJobs++
                else if (job.getJobState() == JobState.OK)
                    finishedJobs++
                else if (job.getJobState() == JobState.UNKNOWN)
                    unknownJobs++
            }
            if (failedJobs > 0) return JobState.FAILED
            if (unknownJobs > 0) return JobState.UNKNOWN
            if (runningJobs > 0) return JobState.RUNNING
            return JobState.OK
        }
        return currentJobState != null ? currentJobState : JobState.UNKNOWN
    }

    Command getLastCommand() {
        return lastCommand
    }


}