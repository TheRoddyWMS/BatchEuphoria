/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs;

/**
 * Created by heinold on 23.02.17.
 */
//public class BEFakeJobID extends BEJobID {
public class BEFakeJobID extends BEJobID.FakeJobID {
    /**
     * Various reasons why a job was not executed and is a fake job.
     */
    public enum FakeJobReason {
        NOT_EXECUTED,
        FILE_EXISTED,
        UNDEFINED,
    }

    private FakeJobReason fakeJobReason;
    private long nanotime;
    private boolean isArray;

    public BEFakeJobID(BEJob job, FakeJobReason fakeJobReason, boolean isArray) {
        super(job);
        this.fakeJobReason = fakeJobReason;
        this.isArray = isArray;
        nanotime = System.nanoTime();
    }

    public BEFakeJobID(BEJob job, boolean isArray) {
        this(job, FakeJobReason.UNDEFINED, isArray);
    }

    public BEFakeJobID(BEJob job, FakeJobReason fakeJobReason) {
        this(job, fakeJobReason, false);
    }

    public BEFakeJobID(BEJob job) {
        this(job, FakeJobReason.UNDEFINED, false);
    }

    /**
     * Fake ids are never valid!
     *
     * @return
     */
    @Override
    public boolean isValidID() {
        return false;
    }

    @Override
    public String getId() {
        return String.format("%s.%s", getShortID(), fakeJobReason.name());
    }

    @Override
    public String getShortID() {
        return String.format("0x%08X%s", nanotime, isArray ? "[]" : "");
    }

    @Override
    public String toString() {
        return getShortID();
    }

    public static boolean isFakeJobID(String jobID) {
        return jobID.startsWith("0x");
    }
}