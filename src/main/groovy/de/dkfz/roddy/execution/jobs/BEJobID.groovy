/*
 * Copyright (c) 2017 eilslabs.
 *  
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode

import java.util.concurrent.atomic.AtomicLong

/**
 * Created by heinold on 01.03.17.
 *
 * JobIDs are born with the JobResults, although JobResults may or may not have a JobID.
 * Jobs don't directly have a JobID. The reference is rather the other way around (see job field of BEJobID).
 */
@EqualsAndHashCode(includeFields = true, includes = ['id'])
@CompileStatic
class BEJobID implements Comparable<BEJobID> {

    private final String id

    private static AtomicLong unknownIdCounter = new AtomicLong(0L)

    BEJobID(String id) {
        this.id = id.split(/\./)[0]
    }

    static BEJobID getNewUnknown() {
        new BEJobID(nextUnknownID())
    }


    protected static String nextUnknownID(String prefix = "UnkownJobID-") {
        return prefix + unknownIdCounter.incrementAndGet()
    }

    boolean isValidID() {
        return !BEFakeJobID.isFakeJobID(id) && id != null && id != "none"
    }

    String getId() {
        return this.id
    }

    String getShortID() {
        return getId()
    }

    String toString() {
        return id
    }

    @Override
    int compareTo(BEJobID o) {
        return this.id.compareTo(o.id)
    }


}