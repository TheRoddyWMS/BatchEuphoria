/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.synchronousexecution;

import de.dkfz.roddy.execution.jobs.JobDependencyID;
import de.dkfz.roddy.execution.jobs.BEJob;

/**
 */
public class DirectCommandDependencyID extends JobDependencyID {
    private final String id;

    protected DirectCommandDependencyID(String id, BEJob job) {
        super(job);
        this.id = id;
    }

    @Override
    public boolean isValidID() {
        return id != null && id != "none";
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getShortID() {
        return id;
    }

    @Override
    public boolean isArrayJob() {
        //TODO
        return false;
    }

    @Override
    public String toString() {
        return "Direct command " + id;
    }
}
