/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs

import de.dkfz.roddy.core.InfoObject
import groovy.transform.CompileStatic

/**
 * Created by heinold on 27.02.17.
 */
@CompileStatic
class FakeJob extends Job {
    FakeJob() {
        super("Fakejob", null, "", null, [], [:], [], [], null)
    }

    public FakeJob(InfoObject context) {
        this()
    }


    static FakeJobID getNotExecutedFakeJob(Job job) {
        return getNotExecutedFakeJob(job, false)
    }

    static FakeJobID getNotExecutedFakeJob(Job job, boolean array) {
        return new FakeJobID(job, FakeJobID.FakeJobReason.NOT_EXECUTED, array)
    }

    static FakeJobID getFileExistedFakeJob(InfoObject infoObject) {
        return getFileExistedFakeJob(new FakeJob(infoObject), false)
    }

    static FakeJobID getFileExistedFakeJob(Job job, boolean array) {
        return new FakeJobID(job, FakeJobID.FakeJobReason.FILE_EXISTED, array)
    }
}