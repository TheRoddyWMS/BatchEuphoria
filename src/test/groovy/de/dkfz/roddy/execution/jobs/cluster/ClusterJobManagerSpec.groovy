/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster

import org.slf4j.Logger
import spock.lang.Specification

import java.lang.reflect.Field
import java.lang.reflect.Method
import java.lang.reflect.Modifier


class ClusterJobManagerSpec extends Specification {

    private static Method prepareLogger(Logger log) {
        Method method = ClusterJobManager.class.getDeclaredMethod("withCaughtAndLoggedException", Closure)
        method.setAccessible(true)

        Field f = ClusterJobManager.class.getDeclaredField("log")
        f.setAccessible(true)
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(f, f.getModifiers() & ~Modifier.FINAL);
        f.set(null, log)
        return method
    }

    def "test catchExceptionAndLog throws exception"() {
        given:
        Logger log = Mock(Logger)
        Method method = prepareLogger(log)

        when:
        Object result = method.invoke(null, { throw new Exception("123"); return "ABC" })

        then:
        result == null
        1 * log.warn("123")
        1 * log.warn(_)
    }

    def "test catchExceptionAndLog returns value"() {
        given:
        Logger log = Mock(Logger)
        Method method = prepareLogger(log)

        when:
        Object result = method.invoke(null, { return "ABC" })

        then:
        result == "ABC"
        0 * log.warn(_)
    }
}
