/*
 * Copyright (c) 2017 eilslabs.
 *  
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.core.InfoObject
import groovy.transform.CompileStatic

/**
 * Created by heinold on 01.03.17.
 *
 * JobIDs are born with the JobResults, although JobResults may or may not have a JobID.
 * Jobs don't directly have a JobID. The reference is rather the other way around (see job field of BEJobID).
 */
@CompileStatic
class BEJobID {

    public final BEJob job
    private final String id

    BEJobID(BEJob job) {
        this.job = job
    }

    BEJobID(String id, BEJob job) {
        this.id = id
        this.job = job
    }

    boolean isValidID() {
        return !BEFakeJobID.isFakeJobID(this.getId()) && getId() != null && getId() != "none"
    }

    public String getId() {
        return this.id
    }

    public String getShortID() {
        return getId()
    }

    String toString() {
        return(id)
    }

    static FakeJobID getNotExecutedFakeJob(BEJob job) {
        return FakeBEJob.getNotExecutedFakeJob(job, false)
    }

    static FakeJobID getNotExecutedFakeJob(BEJob job, boolean array) {
        return FakeBEJob.getNotExecutedFakeJob(job, array)
    }

    static FakeJobID getFileExistedFakeJob(BEJob job, boolean array) {
        return FakeBEJob.getFileExistedFakeJob(job, array)
    }

    static FakeJobID getFileExistedFakeJob(InfoObject infoObject) {
        return FakeBEJob.getFileExistedFakeJob(new FakeBEJob(infoObject), false)
    }

    @Deprecated
    static class FakeJobID extends BEJobID {
        FakeJobID(BEJob job) {
            super(job)
        }
    }

}