/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import de.dkfz.roddy.BEException
import groovy.transform.CompileStatic
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException

import java.time.Duration
import java.time.format.DateTimeParseException

import static junit.framework.Assert.assertEquals

/**
 */
@CompileStatic
class PBSJobManagerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none()

    @Test
    void testParseDuration() throws DateTimeParseException, BEException {
        assertEquals(Duration.ofSeconds(0), PBSJobManager.parseColonSeparatedHHMMSSDuration("00:00:00"))
        assertEquals(Duration.ofHours(24), PBSJobManager.parseColonSeparatedHHMMSSDuration("24:00:00"))
        assertEquals(Duration.ofHours(119), PBSJobManager.parseColonSeparatedHHMMSSDuration("119:00:00"))

        thrown.expect(BEException.class)
        thrown.expectMessage("Duration string is not of the format HH+:MM:SS: '02:42'")
        PBSJobManager.parseColonSeparatedHHMMSSDuration("02:42")
    }
}
