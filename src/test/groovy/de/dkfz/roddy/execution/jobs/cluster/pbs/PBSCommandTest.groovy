/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.config.ResourceSetSize
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic
import org.junit.Test

import java.time.Duration

/**
 * Created by heinold on 26.03.17.
 */
@CompileStatic
class PBSCommandTest {
    @Test
    void testToString() throws Exception {

    }

    @Test
    void testAssembleDependencyStringWithoutDependencies() throws Exception {
        def mapOfParameters = ["a": "a", "b": "b"]
        BEJob job = new BEJob(null, "Test", new File("/tmp/test.sh"),null, null, new ResourceSet(ResourceSetSize.l, new BufferValue(1, BufferUnit.G), 4, 1, new TimeUnit("1h"), null, null, null), [], mapOfParameters, null, JobLog.none(), null)
        PBSCommand cmd = new PBSCommand(null, job, "jobName", null, mapOfParameters, null, "/tmp/test.sh")
        String result = cmd.assembleDependencyString([])
        assert result == ""
    }

}
