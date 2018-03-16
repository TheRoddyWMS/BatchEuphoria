/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs;


import de.dkfz.roddy.execution.jobs.BEJobID;

import java.io.Serializable;

/**
 */
public class PBSJobID extends BEJobID implements Serializable {

    public PBSJobID(String id) {
        super(id);
    }

    @Override
    public boolean isValidID() {
        return super.isValidID() ;
    }

    @Override
    public String getShortID() {
        return getId().split("[.]")[0];
    }

    @Override
    public String toString() {
        return getShortID();
    }
}
