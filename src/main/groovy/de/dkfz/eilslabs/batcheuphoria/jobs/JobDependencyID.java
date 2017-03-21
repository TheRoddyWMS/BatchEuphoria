/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs;

import java.io.Serializable;

/**
 */
public abstract class JobDependencyID extends de.dkfz.roddy.execution.jobs.JobDependencyID implements Serializable {

    protected JobDependencyID(Job job) {
        this.job = job;
    }

    public final Job job;

    public Job getJob() {
        return job;
    }

}
