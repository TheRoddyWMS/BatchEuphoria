/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria

import de.dkfz.eilslabs.batcheuphoria.config.ResourceSet
import de.dkfz.eilslabs.batcheuphoria.config.ResourceSetSize
import de.dkfz.eilslabs.batcheuphoria.execution.ExecutionService
import de.dkfz.eilslabs.batcheuphoria.execution.RestExecutionService
import de.dkfz.eilslabs.batcheuphoria.jobs.Job
import de.dkfz.eilslabs.batcheuphoria.jobs.JobManager
import de.dkfz.eilslabs.batcheuphoria.jobs.JobManagerCreationParameters
import de.dkfz.eilslabs.batcheuphoria.jobs.JobManagerCreationParametersBuilder
import de.dkfz.eilslabs.batcheuphoria.jobs.JobState
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.JobResult
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.LoggerWrapper
import de.dkfz.roddy.tools.RoddyIOHelperMethods
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic

/**
 * Starter class for BE integration tests.
 *
 * Created by heinold on 27.03.17.
 */
@CompileStatic
class IntegrationTestStarter {

    static LoggerWrapper log = LoggerWrapper.getLogger(IntegrationTestStarter)
    static TestExecutionService executionService
    static RestExecutionService restExecutionService
    static String toolScript = "\"#!/bin/bash\\nsleep 15\\n\""
    static File batchEuphoriaTestScript

    static void main(String[] args) {
        checkStartup(args)

        getOptionsFromArgs(args).each { AvailableClusterSystems cs ->
            if(cs == AvailableClusterSystems.lsf){
                initializeTests(args[0],args[1])
                initializeLSFTest(args[2],args[3])
                runTestsFor(cs, restExecutionService)
            }else{
                initializeTests(args[0],args[1])
                runTestsFor(cs, executionService)
            }
        }

        finalizeTests()
    }

    private static List<AvailableClusterSystems> getOptionsFromArgs(String[] args) {
        if(args.length > 4)
            args[4..-1].collect { String it -> it.toLowerCase() as AvailableClusterSystems }
        else
            args[2..-1].collect { String it -> it.toLowerCase() as AvailableClusterSystems }
    }

    private static void runTestsFor(AvailableClusterSystems option, ExecutionService es) {
        JobManager jobManager
        int maxSleep = 5
        log.always("Creating job manager instance for ${option}")

        try {
            jobManager = option.loadClass().getDeclaredConstructor(ExecutionService, JobManagerCreationParameters)
                    .newInstance(es,
                    new JobManagerCreationParametersBuilder()
                            .setCreateDaemon(false)
                            .setTrackUserJobsOnly(true)
                            .build()
            ) as JobManager
        } catch (Exception ex) {
            log.severe("Could not load and instantiate job manager class ${option.className}. Exception: ${ex.printStackTrace()}")
            return
        }

        try {


            log.always("Starting tests for single jobs.")
            Job testJob = new Job("batchEuphoriaTestJob", batchEuphoriaTestScript, null, null, new ResourceSet(ResourceSetSize.s, new BufferValue(10, BufferUnit.m), 1, 1, new TimeUnit("20s"), null, null, null), null, ["a": "value"], null, null, jobManager)
                def jobList = [testJob]

            // run single job and check status
            if(jobManager.isHoldJobsEnabled()) {
                JobResult jr = jobManager.runJob(testJob)
                log.postAlwaysInfo("Started ${jr.jobID.id}")
                ensureProperJobStates(maxSleep, jobList, [JobState.HOLD], jobManager)

                log.always("Start and check status")
                jobManager.startHeldJobs(jobList)
                // Wait for some seconds and see, if the status changes from HOLD to queued or running and from queued to running
                // The queued to running check can take a lot more time. Also the default update time for queries to the job system
                // is too long for tests. We force updates everytime we run queryJobStatus
                ensureProperJobStates(maxSleep, jobList, [JobState.QUEUED, JobState.RUNNING], jobManager)
            } else {
                JobResult jr = jobManager.runJob(testJob)
                log.postAlwaysInfo("Started ${jr.jobID.id}")
                ensureProperJobStates(maxSleep, jobList, [JobState.QUEUED, JobState.RUNNING], jobManager)
            }
            log.always("Abort and check again")
            jobManager.queryJobAbortion(jobList)
            // The job abortion might be a valid command but the cluster system might still try to keep the jobs.
            // We cannot handle problems like a stuck cluster, so let's stick to the basic query and see, if the job
            // ends within some seconds.
            ensureProperJobStates(maxSleep, jobList, [JobState.ABORTED, JobState.OK, JobState.COMPLETED_UNKNOWN, JobState.COMPLETED_UNKNOWN], jobManager)

            // What, if abort was succesful but the jobs are still running? PBS sometimes screws up...

            log.always("Finished single job test\n")
        } catch (Exception ex) {
            log.severe("An error occurd while testing for JobManager type ${option}")
            log.severe(RoddyIOHelperMethods.getStackTraceAsString(ex))
        } finally {

        }

        try {

            log.always("Starting tests for multiple jobs.")

            // run jobs with dependencies
            Job testParent = new Job("batchEuphoriaTestJob_Parent", batchEuphoriaTestScript, null, null, new ResourceSet(ResourceSetSize.s, new BufferValue(10, BufferUnit.m), 1, 1, new TimeUnit("20s"), null, null, null), null, ["a": "value"], null, null, jobManager)
            Job testJobChild1 = new Job("batchEuphoriaTestJob_Child1", batchEuphoriaTestScript, null, null, new ResourceSet(ResourceSetSize.s, new BufferValue(10, BufferUnit.m), 1, 1, new TimeUnit("20s"), null, null, null), null, ["a": "value"], [testParent], null, jobManager)
            Job testJobChild2 = new Job("batchEuphoriaTestJob_Child2", batchEuphoriaTestScript, null, null, new ResourceSet(ResourceSetSize.s, new BufferValue(10, BufferUnit.m), 1, 1, new TimeUnit("20s"), null, null, null), null, ["a": "value"], [testParent, testJobChild1], null, jobManager)

            log.always("Submit jobs.")
            def allJobs = [testParent, testJobChild1, testJobChild2]
            allJobs.each { def jr = jobManager.runJob(it); log.postAlwaysInfo("Started ${jr.jobID.id}") }
            ensureProperJobStates(maxSleep, allJobs, [jobManager.isHoldJobsEnabled() ? JobState.HOLD : JobState.QUEUED], jobManager)

            log.always("Start held jobs.")
            jobManager.startHeldJobs(allJobs)
            ensureProperJobStates(maxSleep, allJobs, [JobState.QUEUED, JobState.RUNNING, JobState.COMPLETED_UNKNOWN, JobState.HOLD], jobManager)

            log.always("Abort jobs.")
            jobManager.queryJobAbortion(allJobs)
            ensureProperJobStates(maxSleep, allJobs, [JobState.ABORTED, JobState.OK, JobState.COMPLETED_UNKNOWN, JobState.COMPLETED_UNKNOWN], jobManager)

            // Should we offer a method to remove held jobs created with a specific prefix? There could e.g. leftovers
            // from failed or debug runs.
        } catch (Exception ex) {
            log.severe("An error occurd while testing for JobManager type ${option}")
            log.severe(RoddyIOHelperMethods.getStackTraceAsString(ex))
        } finally {

        }

        log.severe("Did not test jobManager.queryExtendedJobState")
        log.severe("Did not test jobManager.waitForJobsToFinish")
    }

    private static void ensureProperJobStates(int maxSleep, List<Job> jobList, List<JobState> listOfStatesToCheck, JobManager jobManager) {
        int increments = 8
        int sleep = maxSleep * increments  // 125ms increments, count from 5s to 0 seconds.
        boolean allJobsInCorrectState = false
        List<JobState> lastStates = []
        while (sleep > 0 && !allJobsInCorrectState) {
            lastStates.clear()
            def status = jobManager.queryJobStatus(jobList, true)
            allJobsInCorrectState = true
            for (Job job in jobList) {
                allJobsInCorrectState &= listOfStatesToCheck.contains(status[job])
                lastStates << status[job]
            }
            if (!allJobsInCorrectState) {
                sleep--
                log.severe("Found job states ${status.values().join(" ")}")
            }
        }


        if (!allJobsInCorrectState)
            log.severe("Not all jobs ${jobList.collect { it.jobID }.join(" ")} were in the proper state: [${listOfStatesToCheck.join(" ")}], got last states [${lastStates.join(" ")}]. Make sure, that your job system is working properly.")
    }

    private static void checkStartup(String[] args) {
        if (!args || args.size() < 2) {
            println([
                    "Call the integration test starter with a range of optione like",
                    "java ... ${IntegrationTestStarter.class.name} PBS SGE SLURM LSF",
                    "where the cluster system names are optional but at least one has to be set.",
                    "Available options are (lower case is accepted):",
                    " direct",
                    " pbs",
                    " sge",
                    " slurm",
                    " lsf",
            ].join("\n"))
            System.exit(0)
        }
    }

    private static void initializeTests(String user, String server) {
        try {
            executionService = new TestExecutionService(user, server)
            batchEuphoriaTestScript = File.createTempFile("batchEuphoriaTestScript_", ".sh")
            batchEuphoriaTestScript << toolScript
            executionService.copyFileToRemote(batchEuphoriaTestScript, batchEuphoriaTestScript)
        } catch (Exception ex) {
            log.severe("Could not setup execution service and copy test script.")
        }
    }

    private static void initializeLSFTest(String server, String user) {
        Console cnsl = System.console();
        char[] pwd = null
        if (cnsl != null)
            pwd = cnsl.readPassword("LSF Password: ");

        try {
            restExecutionService = new RestExecutionService(server,user,pwd.toString())
        } catch (Exception ex) {
            log.severe("Could not setup LSF execution service")
        }
    }

    private static ExecutionResult finalizeTests() {
        executionService.execute("rm ${batchEuphoriaTestScript}")
        executionService.executeLocal("rm ${batchEuphoriaTestScript}")
    }
}
