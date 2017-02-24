/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs;

import java.io.Serializable;

/**
 */
public abstract class JobDependencyID implements Serializable {

    protected JobDependencyID(Job job) {
        this.job = job;
    }

    public abstract boolean isValidID();

    public final Job job;



    public abstract String getId();

    public abstract String getShortID();

    public abstract boolean isArrayJob();
    /*
    public static FakeJobID getNotExecutedFakeJob(Job job) {
        return getNotExecutedFakeJob(job, false);
    }
    public static FakeJobID getNotExecutedFakeJob(Job job, boolean array) {
        return new FakeJobID(job, FakeJobReason.NOT_EXECUTED,array);
    }

    public static FakeJobID getFileExistedFakeJob(ExecutionContext context) {
        return getFileExistedFakeJob(new Job.FakeJob(context), false);
    }
    public static FakeJobID getFileExistedFakeJob(Job job, boolean array) {
        return new FakeJobID(job, FakeJobReason.FILE_EXISTED, array);
    }*/
}
