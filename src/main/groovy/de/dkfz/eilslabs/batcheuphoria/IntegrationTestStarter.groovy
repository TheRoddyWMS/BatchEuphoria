/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria

import de.dkfz.eilslabs.batcheuphoria.config.ResourceSet
import de.dkfz.eilslabs.batcheuphoria.config.ResourceSetSize
import de.dkfz.eilslabs.batcheuphoria.execution.cluster.pbs.PBSJobManager
import de.dkfz.eilslabs.batcheuphoria.jobs.Job
import de.dkfz.eilslabs.batcheuphoria.jobs.JobManagerCreationParametersBuilder
import de.dkfz.roddy.execution.jobs.JobResult
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.LoggerWrapper
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic

/**
 * Starter class for BE integration tests.
 *
 * Created by heinold on 27.03.17.
 */
@CompileStatic
class IntegrationTestStarter {

    static LoggerWrapper loggerWrapper = LoggerWrapper.getLogger(IntegrationTestStarter)
    static TestExecutionService executionService

    static void main(String[] args) {
        checkStartup(args)

        executionService = new TestExecutionService(args[0], args[1])

        getOptionsFromArgs(args).each { runTestsFor(it) }
    }

    private static void runTestsFor(AvailableClusterSystems option) {
        File batchEuphoriaTestScript
        try {
            if (option == AvailableClusterSystems.pbs) {

                // Create a temporary script which can be run by the job system
                PBSJobManager jobManager = new PBSJobManager(executionService, new JobManagerCreationParametersBuilder().setCreateDaemon(false).build())

                batchEuphoriaTestScript = File.createTempFile("batchEuphoriaTestScript_", ".sh")
                batchEuphoriaTestScript << "#!/bin/bash\nsleep 15\n"
                executionService.copyFileToRemote(batchEuphoriaTestScript, batchEuphoriaTestScript)
                Job testJob = new Job("batchEuphoriaTestJob", batchEuphoriaTestScript, null, new ResourceSet(ResourceSetSize.s, new BufferValue(10, BufferUnit.m), 1, 1, new TimeUnit("20s"), null, null, null), null, ["a": "value"], null, null, jobManager)

                // run single job
                JobResult jr = jobManager.runJob(testJob)
                jobManager.startHeldJobs([jr.job])

                // run jobs with dependencies
                Job testParent = new Job("batchEuphoriaTestJob", batchEuphoriaTestScript, null, new ResourceSet(ResourceSetSize.s, new BufferValue(10, BufferUnit.m), 1, 1, new TimeUnit("20s"), null, null, null), null, ["a": "value"], null, null, jobManager)
                Job testJobChild1 = new Job("batchEuphoriaTestJob", batchEuphoriaTestScript, null, new ResourceSet(ResourceSetSize.s, new BufferValue(10, BufferUnit.m), 1, 1, new TimeUnit("20s"), null, null, null), null, ["a": "value"], [testParent], null, jobManager)
                Job testJobChild2 = new Job("batchEuphoriaTestJob", batchEuphoriaTestScript, null, new ResourceSet(ResourceSetSize.s, new BufferValue(10, BufferUnit.m), 1, 1, new TimeUnit("20s"), null, null, null), null, ["a": "value"], [testParent, testJobChild1], null, jobManager)

                // startHeldJobs
                def allJobs = [testParent, testJobChild1, testJobChild2]
                allJobs.each { jobManager.runJob(it) }
                jobManager.startHeldJobs(allJobs)

                // abortJobs
                jobManager.queryJobAbortion([testJob] + allJobs)

                // rollBack
                // Abort and what? Rollback seems more like a client function
            } else {
                loggerWrapper.severe("Testing $option is not supported yet.")
            }
        } finally {
            executionService.execute("rm ${batchEuphoriaTestScript}")
        }
    }

    private static List<AvailableClusterSystems> getOptionsFromArgs(String[] args) {
        args[2..-1].collect { String it -> it.toLowerCase() as AvailableClusterSystems }
    }

    private static void checkStartup(String[] args) {
        if (!args || args.size() < 2) {
            println([
                    "Call the integration test starter with a range of optione like",
                    "java ... ${IntegrationTestStarter.class.name} PBS SGE SLURM LSF",
                    "where the cluster system names are optional but at least one has to be set.",
                    "Available options are (lower case is accepted):",
                    " direct",
                    " pbs:server",
                    " sge:server",
                    " slurm:server",
                    " lsf:server",
            ].join("\n"))
            System.exit(0)
        }
    }
}
