/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */
package de.dkfz.roddy.execution.jobs

import com.google.common.collect.LinkedHashMultimap
import groovy.transform.CompileStatic

import java.text.ParseException
import java.util.regex.Pattern

/**
 */
@CompileStatic
class ProcessingParameters implements Serializable {

    private LinkedHashMultimap<String, String> parameters = null

    ProcessingParameters(LinkedHashMultimap<String, String> parameters) {
        assert (null != parameters)
        this.parameters = parameters
    }

    /**
     * Parse a string into a ProcessingParameter object. Assumption is that the string can first be separated by /\s+-/ (i.e. a sequence of white
     * spaces followed by a parameter indicator '-' (as in Unix) followed by a sequence of non-space non-'=' characters indicating the parameter name
     * and finally an optionally single or double quoted parameter value. The ' value' or '=value' part is optional. The missing value is then
     * returned as parameter name as key with null-value.
     *
     * @param parameterString
     * @return
     */
    static ProcessingParameters fromString(String parameterString) {
        def pattern = Pattern.compile(/(?<optionName>[^\s=]+)(?:[\s+=](?<optionValue>.+?\s*))?/)
        LinkedHashMultimap<String, String> parameters = LinkedHashMultimap.create()
        parameterString.split(/(^|\s+)(?=-)/).findAll { it != '' }.eachWithIndex { String option, int i ->
            def matcher = pattern.matcher(option)
            if (matcher.matches()) {
                parameters.put(matcher.group("optionName"), matcher.group("optionValue"))
                matcher.reset()
            } else {
                matcher.reset()
                throw new ParseException("Could not match parameter ${i} in '${parameterString}'" as String, i)
            }
        }
        return new ProcessingParameters(parameters)
    }

    String getProcessingCommandString() {
        // Careful with this method. We had one problem where an Integer value was in one of the Collections inside the map.
        // In this particular case, Groovy said there is something wrong with the Integer [1].
        (parameters.asMap() as Map<String, Collection<String>>).collect { k, vs ->
            vs.collect { v ->
                k + ' ' + v
            }.join(' ')
        }.join(' ')
    }

    @Override
    String toString() {
        return "" + processingCommandString
    }
}
