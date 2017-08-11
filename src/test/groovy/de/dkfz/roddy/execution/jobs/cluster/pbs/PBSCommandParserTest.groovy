/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.JobManagerCreationParametersBuilder
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic
import org.junit.BeforeClass
import org.junit.Test

/**
 * Created by heinold on 04.04.17.
 */
@CompileStatic
class PBSCommandParserTest {

    static BatchEuphoriaJobManager testJobManager

    @BeforeClass
    static void setup() {
        testJobManager = new PBSJobManager(null, (new JobManagerCreationParametersBuilder()).setCreateDaemon(false).build())
    }

    @Test
    void testConstructAndParseSimpleJob() {
        // TODO Introduce more complex tests with more qsub lines.
        String commandString = [
                "120016, qsub -N r170402_171935425_A100_indelCalling -h",
                "-W depend=afterok:120015",
                "-o /data/michael/temp/roddyLocalTest/testproject/rpp/A100/roddyExecutionStore/exec_170402_171935425_heinold_indelCalling",
                "-j oe  -l mem=16384M -l walltime=02:02:00:00 -l nodes=1:ppn=8",
                "-v PARAMETER_FILE=/data/michael/temp/roddyLocalTest/testproject/rpp/A100/roddyExecutionStore/exec_170402_171935425_heinold_indelCalling/r170402_171935425_A100_indelCalling_1.parameters",
                "/data/michael/temp/roddyLocalTest/testproject/rpp/A100/roddyExecutionStore/exec_170402_171935425_heinold_indelCalling/analysisTools/indelCallingWorkflow/indelCalling.sh"
        ].join(" ")

        def commandParser = new PBSCommandParser(commandString)

        assert commandParser.script == "/data/michael/temp/roddyLocalTest/testproject/rpp/A100/roddyExecutionStore/exec_170402_171935425_heinold_indelCalling/analysisTools/indelCallingWorkflow/indelCalling.sh"
        assert commandParser.jobName == "r170402_171935425_A100_indelCalling"
        assert commandParser.id == "120016"
        assert commandParser.walltime == "02:02:00:00"
        assert commandParser.memory == "16384"
        assert commandParser.nodes == "1"
        assert commandParser.cores == "8"
        assert commandParser.parameters  == ["PARAMETER_FILE":"/data/michael/temp/roddyLocalTest/testproject/rpp/A100/roddyExecutionStore/exec_170402_171935425_heinold_indelCalling/r170402_171935425_A100_indelCalling_1.parameters"]
        assert commandParser.dependencies == ["120015"]

        def gji = testJobManager.parseGenericJobInfo(commandString)
        assert gji.jobName == "r170402_171935425_A100_indelCalling"
        assert gji.askedResources.getCores() == 8
        assert gji.askedResources.getNodes() == 1
        assert gji.askedResources.getWalltime() == new TimeUnit("02:02:00:00")
        assert gji.askedResources.getMem().toLong() == 16384
        assert gji.parameters  == ["PARAMETER_FILE":"/data/michael/temp/roddyLocalTest/testproject/rpp/A100/roddyExecutionStore/exec_170402_171935425_heinold_indelCalling/r170402_171935425_A100_indelCalling_1.parameters"]
        assert gji.parentJobIDs == ["120015"]
    }


}