/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */
package de.dkfz.roddy.execution.jobs

import com.google.common.collect.LinkedHashMultimap
import groovy.transform.CompileStatic
import org.junit.Test

import static org.junit.Assert.*

@CompileStatic
class ProcessingParametersTest {

    @Test
    void fromString() throws Exception {
        List<String> values = ["-l", "abc", "-l", "def", "-W", null, "-v", null]
        LinkedHashMultimap<String, String> expected = LinkedHashMultimap.create()
        values.collate(2).each { String k, String v -> expected.put(k, v) }
        assertEquals(new ProcessingParameters(expected).toString(), ProcessingParameters.fromString(values.findAll { it }.join(" ")).toString())
    }

}