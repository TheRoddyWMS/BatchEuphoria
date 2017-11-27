/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import de.dkfz.roddy.BEException
import spock.lang.Specification

import java.time.Duration

class PBSJobManagerSpec extends Specification {

    void testParseDuration() {
        expect:
        result == PBSJobManager.parseColonSeparatedHHMMSSDuration(input)

        where:
        input       || result
        "00:00:00"  || Duration.ofSeconds(0)
        "24:00:00"  || Duration.ofHours(24)
        "119:00:00" || Duration.ofHours(119)
    }

    void "testParseDuration fails"() {
        when:
        PBSJobManager.parseColonSeparatedHHMMSSDuration("02:42")

        then:
        BEException e = thrown(BEException)
        e.message == "Duration string is not of the format HH+:MM:SS: '02:42'"
    }
}
