/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.batcheuphoria.jobs

import de.dkfz.roddy.execution.jobs.BEJobID
import spock.lang.Specification

class BEJobIDSpec  extends Specification {
    def "test equals and hashcode"() {
        given:
        BEJobID job1a = new BEJobID("1")
        BEJobID job1b = new BEJobID("1")
        BEJobID job2 = new BEJobID("2")

        expect:
        job1a == job1b
        job1a != job2
        job1a.hashCode() == job1b.hashCode()
        job1a.hashCode() != job2.hashCode()
    }
}
