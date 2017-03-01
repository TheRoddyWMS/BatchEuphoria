/*
 * Copyright (c) 2017 eilslabs.
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

}
