/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs;


import de.dkfz.roddy.execution.jobs.BEJob;
import de.dkfz.roddy.execution.jobs.BEJobID;

import java.io.Serializable;

/**
 */
public class PBSJobID extends BEJobID implements Serializable {
    private String id;

    public PBSJobID(BEJob job, String id) {
        super(job);
        this.id = id;
    }

    @Override
    public boolean isValidID() {
        return super.isValidID() && id != null && id != "none";
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
