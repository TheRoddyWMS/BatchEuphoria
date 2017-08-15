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
        super(null, "Fakejob", null, null, "", null, [], [:], null)
    }

    FakeBEJob(InfoObject context) {
        this()
    }


    static BEFakeJobID getNotExecutedFakeJobID() {
        return getNotExecutedFakeJobID(false)
    }

    static BEFakeJobID getNotExecutedFakeJobID(boolean array) {
        return new BEFakeJobID(BEFakeJobID.FakeJobReason.NOT_EXECUTED, array)
    }

    static BEFakeJobID getFileExistedFakeJobID(InfoObject infoObject) {
        return getFileExistedFakeJobID(false)
    }

    static BEFakeJobID getFileExistedFakeJobID(boolean array) {
        return new BEFakeJobID(BEFakeJobID.FakeJobReason.FILE_EXISTED, array)
    }
}