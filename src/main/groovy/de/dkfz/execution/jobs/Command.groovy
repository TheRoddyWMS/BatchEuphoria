/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.execution.jobs

import groovy.transform.CompileStatic

/**
 * Base class for all types of commands.
 * <p>
 * PBSCommand extends this. Also SGECommand and so on.
 * <p>
 * A job is executed via a command. The command represents the job on the cluster / system side.
 *
 * @author michael
 */
@CompileStatic
public abstract class Command {


    public void setJob(Job job) {
        this.creatingJob = job;
    }
/*
    public static class DummyCommand extends Command {

        private String jobName;

        public DummyCommand(Job job, ExecutionContext run, String jobName, boolean isArray) {
            super(job, run, "dummy_" + getNextIDCountValue(), null);
            this.jobName = jobName;
            if (isArray) {
                setExecutionID(JobDependencyID.getNotExecutedFakeJob(job, true));
            } else {
                setExecutionID(JobDependencyID.getNotExecutedFakeJob(job));
            }
        }


        @Override
        public String toString() {
            return String.format("Command of class %s with id %s and name %s", this.getClass().getName(), getID(), jobName);
        }

    }
*/
    /**
     * Static incremental counter for pipeline commands.
     */
    protected static volatile int idCounter = -1;
    /**
     * The id of this command.
     */
    private final String id;
    /**
     * The id which was created upon de.dkfz.execution by the job system.
     */
    protected JobDependencyID executionID;

    /**
     * The job which created this command. Can be null!
     */
    protected Job creatingJob;


    /**
     * Parameters for the qsub command
     */
    protected Map<String, String> parameters;

    protected File parameterFile;

    protected Command(Job job, String id,File parameterFile, Map<String, String> parameters) {
        this.parameters = parameters ?: new LinkedHashMap<String, String>();
        this.creatingJob = job;
        this.id = id;
        this.parameterFile = parameterFile;
        JobManager.getInstance()?.addCommandToList(this);
    }

    protected Command(Job job,File parameterFile, String id) {
        this(job,id, parameterFile, null);
        this.creatingJob = job;
    }

    protected static synchronized int getNextIDCountValue() {
        return ++idCounter;
    }


    public final void setExecutionID(JobDependencyID id) {
        this.executionID = id;
    }

    public final boolean wasExecuted() {
        return executionID.isValidID();//executionID != null && executionID.trim().length() > 0;
    }

    public final JobDependencyID getExecutionID() {
        return executionID;
    }

    public final String getID() {
        return id;
    }

    public final Job getJob() {
        return creatingJob;
    }

    public final String getFormattedID() {
        return String.format("command:0x%08X", id);
    }

    public File getParameterFile() {
        return parameterFile;
    }

    /*
    public List<ConfigurationValue> getParametersForParameterFile() {
        List<ConfigurationValue> allParametersForFile = new LinkedList<>();
        if (parameters.size() > 0) {
            for (String parm : parameters.keySet()) {
                String val = parameters.get(parm);
                if (val.contains(StringConstants.DOLLAR_LEFTBRACE) && val.contains(StringConstants.BRACE_RIGHT)) {
                    val = val.replace(StringConstants.DOLLAR_LEFTBRACE, "#{"); // Replace variable names so they can be passed to qsub.
                }
                String key = parm;
                allParametersForFile.add(new ConfigurationValue(key, val));
            }
        }
        return allParametersForFile;
    }*/

    /**
     * Local commands are i.e. blocking, whereas PBSCommands are not.
     * The default is false.
     *
     * @return
     */
    public boolean isBlockingCommand() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("Command of class %s with id %s", this.getClass().getName(), getID());
    }
}
