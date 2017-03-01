/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs

import groovy.transform.CompileStatic

/**
 * Created by heinold on 23.02.17.
 */
@CompileStatic
class DummyCommand extends Command {

    /**
     * Static incremental counter for pipeline commands.
     */
    protected static volatile int idCounter = -1

    protected static synchronized int getNextIDCountValue() {
        return ++idCounter
    }

    private String jobName;

    DummyCommand(JobManager parentJobManager, Job job, String jobName, boolean isArray) {
        super(parentJobManager, job, "dummy_" + getNextIDCountValue(), null, null);
        this.jobName = jobName;
        if (isArray) {
            setExecutionID(FakeJob.getNotExecutedFakeJob(job, true));
        } else {
            setExecutionID(FakeJob.getNotExecutedFakeJob(job));
        }
    }

    @Override
    public String toString() {
        return String.format("Command of class %s with id %s and name %s", this.getClass().getName(), getID(), jobName);
    }
}