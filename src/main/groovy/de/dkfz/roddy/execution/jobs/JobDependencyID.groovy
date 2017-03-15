/*
 * Copyright (c) 2017 eilslabs.
 *  
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.eilslabs.batcheuphoria.jobs.FakeJob
import de.dkfz.eilslabs.batcheuphoria.jobs.FakeJobID
import de.dkfz.eilslabs.batcheuphoria.jobs.Job
import de.dkfz.roddy.core.InfoObject
import groovy.transform.CompileStatic

/**
 * Compatibility interface for Roddy and Roddy plugins.
 * Without it, all plugins depending on 2.3 would need to be adapted and recompiled!
 * Created by heinold on 01.03.17.
 */
@Deprecated
@CompileStatic
abstract class JobDependencyID {

    abstract boolean isValidID()

    abstract Job getJob()

    abstract String getId()

    abstract String getShortID()

    abstract boolean isArrayJob()

    static FakeJobID getNotExecutedFakeJob(Job job) {
        return FakeJob.getNotExecutedFakeJob(job, false)
    }

    static FakeJobID getNotExecutedFakeJob(Job job, boolean array) {
        return FakeJob.getNotExecutedFakeJob(job, array)
    }

    static FakeJobID getFileExistedFakeJob(Job job, boolean array) {
        return FakeJob.getFileExistedFakeJob(job, array)
    }

    static FakeJobID getFileExistedFakeJob(InfoObject infoObject) {
        return FakeJob.getFileExistedFakeJob(new FakeJob(infoObject), false)
    }

    @Deprecated
    static abstract class FakeJobID extends de.dkfz.eilslabs.batcheuphoria.jobs.JobDependencyID {
        FakeJobID(Job job) {
            super(job)
        }
    }

}