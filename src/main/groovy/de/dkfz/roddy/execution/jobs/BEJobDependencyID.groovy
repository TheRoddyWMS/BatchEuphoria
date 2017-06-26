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
 */
@CompileStatic
abstract class BEJobDependencyID {

    public final BEJob job;

    abstract boolean isValidID()

    abstract String getId()

    abstract String getShortID()

    abstract boolean isArrayJob()

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

    BEJobDependencyID(BEJob job) {
        this.job = job
    }

    @Deprecated
    static abstract class FakeJobID extends BEJobDependencyID {
        FakeJobID(BEJob job) {
            super(job)
        }
    }

}