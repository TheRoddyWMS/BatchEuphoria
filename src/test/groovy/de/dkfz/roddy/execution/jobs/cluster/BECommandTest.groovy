/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster

import groovy.transform.CompileStatic

/**
 * Created by heinold on 14.07.16.
 */
@CompileStatic
class BECommandTest extends GroovyTestCase {
    void testGetParametersForParameterFile() {
        // Roddy specfic? How do we handle parameter files?
//        def context = MockupExecutionContextBuilder.createSimpleContext(BECommandTest)
//        Command mock = new Command(new BEJob.FakeBEJob(context), context, "MockupCommand", [
//                "ParmA": "Value",
//                "arr"  : "(a b c )",
//                "int"  : "1"
//        ]) {}
//
//        def parameterList = mock.getParametersForParameterFile()
//        assert parameterList == [
//                new ConfigurationValue("ParmA", "Value", "string"),
//                new ConfigurationValue("arr", "(a b c )", "bashArray"),
//                new ConfigurationValue("int", "1", "integer"),
//        ]
    }
}
