/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.core.InfoObject
import groovy.transform.CompileStatic

/**
 * Created by heinold on 27.02.17.
 */
@CompileStatic
class FakeBEJob extends BEJob {
    FakeBEJob() {
        super("Fakejob", null,null, "", null, [], [:], [], [], null)
    }

    public FakeBEJob(InfoObject context) {
        this()
    }


    static FakeJobID getNotExecutedFakeJob(BEJob job) {
        return getNotExecutedFakeJob(job, false)
    }

    static FakeJobID getNotExecutedFakeJob(BEJob job, boolean array) {
        return new FakeJobID(job, FakeJobID.FakeJobReason.NOT_EXECUTED, array)
    }

    static FakeJobID getFileExistedFakeJob(InfoObject infoObject) {
        return getFileExistedFakeJob(new FakeBEJob(infoObject), false)
    }

    static FakeJobID getFileExistedFakeJob(BEJob job, boolean array) {
        return new FakeJobID(job, FakeJobID.FakeJobReason.FILE_EXISTED, array)
    }
}