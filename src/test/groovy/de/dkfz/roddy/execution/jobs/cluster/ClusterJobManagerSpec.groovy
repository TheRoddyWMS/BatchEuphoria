/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster

import de.dkfz.roddy.BEException
import org.slf4j.Logger
import spock.lang.Specification

import java.lang.reflect.Field
import java.lang.reflect.Method
import java.lang.reflect.Modifier
import java.time.Duration


class ClusterJobManagerSpec extends Specification {

    void "test parseColonSeparatedHHMMSSDuration, parse duration"() {
        expect:
        ClusterJobManager.parseColonSeparatedHHMMSSDuration(input) == parsedDuration

        where:
        input       | parsedDuration
        "00:00:00"  | Duration.ofSeconds(0)
        "24:00:00"  | Duration.ofHours(24)
        "119:00:00" | Duration.ofHours(119)
    }

    void "test parseColonSeparatedHHMMSSDuration, parse duration fails"() {
        when:
        ClusterJobManager.parseColonSeparatedHHMMSSDuration("02:42")

        then:
        BEException e = thrown(BEException)
        e.message == "Duration string is not of the format HH+:MM:SS: '02:42'"
    }

    private static Method prepareClassLogger(Class _class, Logger log) {
        Field f = _class.getDeclaredField("log")
        f.setAccessible(true)

        Field modifiersField = f.class.getDeclaredField("modifiers")
        modifiersField.setAccessible(true)
        modifiersField.setInt(f, f.getModifiers() & ~Modifier.FINAL)
        f.set(null, log)
    }

    def "test withCaughtAndLoggedException throws exception"() {
        given:
        Logger log = Mock(Logger)
        prepareClassLogger(ClusterJobManager, log)

        when:
        Object result = ClusterJobManager.withCaughtAndLoggedException { throw new Exception("123") }

        then:
        result == null
        1 * log.warn("123")
        1 * log.warn(_)
    }

    def "test withCaughtAndLoggedException returns value"() {
        given:
        Logger log = Mock(Logger)
        prepareClassLogger(ClusterJobManager, log)

        when:
        Object result = ClusterJobManager.withCaughtAndLoggedException { return "ABC" }

        then:
        result == "ABC"
        0 * log.warn(_)
    }
}
