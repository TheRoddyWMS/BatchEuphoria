/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.execution.jobs.cluster.pbs;


import de.dkfz.execution.jobs.Job;
import de.dkfz.execution.jobs.JobDependencyID;

import java.io.Serializable;

/**
 */
public class PBSJobDependencyID extends JobDependencyID implements Serializable {
    private String id;

    public PBSJobDependencyID(Job job, String id) {
        super(job);
        this.id = id;
    }

    @Override
    public boolean isValidID() {
        return id != null && id != "none";
    }

    @Override
    public boolean isArrayJob() {
        return id.contains("[].");
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getShortID() {
        return id.split("[.]")[0];
    }

    @Override
    public String toString() {
        return getShortID();
    }
}
