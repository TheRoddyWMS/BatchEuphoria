/*
 * Copyright (c) 2021  German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import groovy.transform.CompileStatic

@CompileStatic
class DummyCommand extends Command {

    /**
     * Static incremental counter for pipeline commands.
     */
    protected static volatile int idCounter = -1

    protected static synchronized int getNextIDCountValue() {
        return ++idCounter
    }

    private String jobName

    DummyCommand(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName, boolean isArray) {
        super(parentJobManager, job, "dummy_" + getNextIDCountValue(), null)
        this.jobName = jobName
        setJobID(new BEFakeJobID(BEFakeJobID.FakeJobReason.NOT_EXECUTED))
    }

    @Override
    String toString() {
        return String.format("Command of class %s with jobName %s and name %s", this.getClass().getName(), getID(), jobName);
    }

    @Override
    String toBashCommandString() {
        throw new RuntimeException("Not implemented!")
    }
}
