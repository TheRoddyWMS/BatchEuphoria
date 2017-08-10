/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.direct.synchronousexecution;

import de.dkfz.roddy.execution.jobs.BEJobID;
import de.dkfz.roddy.execution.jobs.BEJob;

/**
 */
public class DirectCommandID extends BEJobID {

    protected DirectCommandID(String id, BEJob job) {
        super(id, job);
    }

    @Override
    public String toString() {
        return "Direct command " + getId();
    }
}
