/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import java.util.regex.Pattern

import static de.dkfz.roddy.execution.jobs.BEFakeJobID.FakeJobReason.values

/**
 * Created by heinold on 23.02.17.
 */
class BEFakeJobID extends BEJobID {
    /**
     * Various reasons why a job was not executed and is a fake job.
     */
    enum FakeJobReason {
        NOT_EXECUTED,
        FILE_EXISTED,
        UNDEFINED,
    }

    private FakeJobReason fakeJobReason
    private long nanotime

    BEFakeJobID(FakeJobReason fakeJobReason, boolean isArray = false) {
        super(nextUnknownID(fakeJobReason.toString() + "-"))
        this.fakeJobReason = fakeJobReason
        nanotime = System.nanoTime()
    }

    BEFakeJobID() {
        this(FakeJobReason.UNDEFINED, false)
    }

    /**
     * Fake ids are never valid!
     *
     * @return
     */
    @Override
    boolean isValidID() {
        return false
    }

    @Override
    String toString() {
        return getShortID()
    }

    private final static List<Pattern> patterns = values().collect { enumVal ->
        return Pattern.compile('^' + enumVal.name() + '-\\d+')
    }

    boolean isFakeJobID() {
        isFakeJobID(this.id)
    }

    static boolean isFakeJobID(String id) {
        return patterns.find { pat ->
            pat.matcher(id)
        } as Boolean
    }

}