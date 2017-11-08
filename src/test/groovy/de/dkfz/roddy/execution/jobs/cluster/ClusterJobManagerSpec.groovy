/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster

import de.dkfz.roddy.tools.LoggerWrapper
import spock.lang.Specification

import java.lang.reflect.Field
import java.lang.reflect.Method
import java.lang.reflect.Modifier


class ClusterJobManagerSpec extends Specification {

    private static Method prepareLogger(LoggerWrapper loggerWrapper) {
        Method method = ClusterJobManager.class.getDeclaredMethod("catchExceptionAndLog", Closure)
        method.setAccessible(true)

        Field f = ClusterJobManager.class.getDeclaredField("logger")
        f.setAccessible(true)
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(f, f.getModifiers() & ~Modifier.FINAL);
        f.set(null, loggerWrapper)
        return method
    }

    def "test catchExceptionAndLog throws exception"() {
        given:
        LoggerWrapper logger = Mock(LoggerWrapper)
        Method method = prepareLogger(logger)

        when:
        Object result = method.invoke(null, { throw new Exception("123"); return "ABC" })

        then:
        result == null
        1 * logger.warning("123")
        1 * logger.warning(_)
    }

    def "test catchExceptionAndLog returns value"() {
        given:
        LoggerWrapper logger = Mock(LoggerWrapper)
        Method method = prepareLogger(logger)

        when:
        Object result = method.invoke(null, { return "ABC" })

        then:
        result == "ABC"
        0 * logger.warning(_)
    }
}
