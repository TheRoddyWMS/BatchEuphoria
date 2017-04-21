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
    static String testScript = "\"#!/bin/bash\\nsleep 15\\n\""
    static File batchEuphoriaTestScript
    static ResourceSet resourceSet = new ResourceSet(ResourceSetSize.s, new BufferValue(10, BufferUnit.m), 1, 1, new TimeUnit("m"), null, null, null)

    static void main(String[] args) {
        IntegrationTestInput testInput= new IntegrationTestInput()
        checkStartup(args,testInput)

        initializeTests(testInput.account,testInput.server)

        if(testInput.clusterSystem == AvailableClusterSystems.lsf){
            initializeLSFTest(testInput.restServer, testInput.restAccount)
            runTestsFor(testInput.clusterSystem, restExecutionService)
        }else{
            runTestsFor(testInput.clusterSystem, executionService)
        }

        finalizeTests()
    }


    private static void runTestsFor(AvailableClusterSystems option, ExecutionService es) {
        JobManager jobManager
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
            log.severe("Could not load and instantiate job manager class ${option.className}.",ex)
            return
        }

        Job testJobwithScript = new Job("batchEuphoriaTestJob", null, testScript, null, resourceSet, null, ["a": "value"], null, null, jobManager)
        singleJobTest(jobManager, testJobwithScript)

        Job testJobwithFile = new Job("batchEuphoriaTestJob", batchEuphoriaTestScript, null, null, resourceSet, null, ["a": "value"], null, null, jobManager)
        singleJobTest(jobManager, testJobwithFile)

        Job testParent = new Job("batchEuphoriaTestJob_Parent", batchEuphoriaTestScript, null, null, resourceSet, null, ["a": "value"], null, null, jobManager)
        Job testJobChild1 = new Job("batchEuphoriaTestJob_Child1", batchEuphoriaTestScript, null, null, resourceSet, null, ["a": "value"], [testParent], null, jobManager)
        Job testJobChild2 = new Job("batchEuphoriaTestJob_Child2", batchEuphoriaTestScript, null, null, resourceSet, null, ["a": "value"], [testParent, testJobChild1], null, jobManager)
        multipleJobsTest(jobManager,[testParent,testJobChild1,testJobChild2])

        log.severe("Did not test jobManager.queryExtendedJobState")
        log.severe("Did not test jobManager.waitForJobsToFinish")
    }


    private static void singleJobTest(JobManager jobManager, Job testJob){
        int maxSleep = 5
        try {
            log.always("Starting tests for single jobs.")

            def jobList = [testJob]

            // run single job and check status
            JobResult jr = jobManager.runJob(testJob)
            if(jobManager.isHoldJobsEnabled()) {
                log.postAlwaysInfo("Started ${jr.jobID.id}")
                ensureProperJobStates(maxSleep, jobList, [JobState.HOLD], jobManager)

                log.always("Start and check status")
                jobManager.startHeldJobs(jobList)
                // Wait for some seconds and see, if the status changes from HOLD to queued or running and from queued to running
                // The queued to running check can take a lot more time. Also the default update time for queries to the job system
                // is too long for tests. We force updates everytime we run queryJobStatus
                ensureProperJobStates(maxSleep, jobList, [JobState.QUEUED, JobState.RUNNING], jobManager)
            } else {
                log.postAlwaysInfo("Started ${jr.jobID.id}")
                ensureProperJobStates(maxSleep, jobList, [JobState.QUEUED, JobState.HOLD, JobState.RUNNING], jobManager)
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
            log.severe("An error occurd while testing for JobManager type ${jobManager.getClass()}")
            log.severe(RoddyIOHelperMethods.getStackTraceAsString(ex))
        } finally {

        }
    }

    private static void multipleJobsTest(JobManager jobManager, List<Job> testJobs){
        int maxSleep = 5
        try {

            log.always("Starting tests for multiple jobs.")

            // run jobs with dependencies
            log.always("Submit jobs.")
            testJobs.each { def jr = jobManager.runJob(it); log.postAlwaysInfo("Started ${jr.jobID.id}") }
            ensureProperJobStates(maxSleep, testJobs, [jobManager.isHoldJobsEnabled() ? JobState.HOLD : JobState.QUEUED], jobManager)

            log.always("Start held jobs.")
            jobManager.startHeldJobs(testJobs)
            ensureProperJobStates(maxSleep, testJobs, [JobState.QUEUED, JobState.RUNNING, JobState.COMPLETED_UNKNOWN, JobState.HOLD], jobManager)

            log.always("Abort jobs.")
            jobManager.queryJobAbortion(testJobs)
            ensureProperJobStates(maxSleep, testJobs, [JobState.ABORTED, JobState.OK, JobState.COMPLETED_UNKNOWN, JobState.COMPLETED_UNKNOWN], jobManager)

            // Should we offer a method to remove held jobs created with a specific prefix? There could e.g. leftovers
            // from failed or debug runs.
        } catch (Exception ex) {
            log.severe("An error occurd while testing for JobManager type ${jobManager.getClass().toString()}")
            log.severe(RoddyIOHelperMethods.getStackTraceAsString(ex))
        } finally {

        }
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

    private static void checkStartup(String[] args, IntegrationTestInput testInput) {
        def cli = new CliBuilder(usage: '-s "server" -a "account" -rs "rest_server" -ra "rest_account" -c [lsf,pbs] ')
        cli.options.addOption("h","help",false, "Show usage information")
        cli.options.addOption("c","cluster",true, "Set the used cluster system e.g. -c pbs. Currently only lsf or pbs")
        cli.options.addOption("s","server",true, "Head node of the set cluster system")
        cli.options.addOption("a","account",true, "User account which has access to the set cluster system")
        cli.options.addOption("ra","restaccount",true, "REST serivce account (only for LSF REST required)")
        cli.options.addOption("rs","restserver",true, "REST service URL e.g. http(s)://localhost:8080/platform/ws (only for LSF REST required)")
        //cli.options.addOption(longOpt: 'debug' ,false, 'enable debugging')


        OptionAccessor opt = cli.parse(args)

        if(!opt) {
            System.exit(0)
        }
        // print usage if -h, --help, or no argument is given
        if(opt.getProperty("h") || opt.arguments().isEmpty()) {
            cli.usage()
        }

        if( opt.getProperty("c") ) {
            testInput.setClusterSystem((opt.getProperty("c") as String).toLowerCase() as AvailableClusterSystems)
        }

        if( opt.getProperty("s") && opt.getProperty("a")) {
            testInput.setServer(opt.getProperty("s") as String)
            testInput.setAccount(opt.getProperty("a") as String)
        }else{
            if(!opt.getProperty("a") && opt.getProperty("s")) {
                cli.usage()
                System.exit(0)
            }
            if(opt.getProperty("a") && !opt.getProperty("s")) {
                cli.usage()
                System.exit(0)
            }
        }

        if( opt.getProperty("ra") && opt.getProperty("rs") ) {
            testInput.setRestAccount(opt.getProperty("ra") as String)
            testInput.setRestServer(opt.getProperty("rs") as String)
        }else{
            if(!opt.getProperty("ra") && opt.getProperty("rs")) {
                cli.usage()
                System.exit(0)
            }
            if(opt.getProperty("ra") && !opt.getProperty("rs")) {
                cli.usage()
                System.exit(0)
            }
        }
    }

    private static void initializeTests(String user, String server) {
        try {
            executionService = new TestExecutionService(user, server)
            batchEuphoriaTestScript = File.createTempFile("batchEuphoriaTestScript_", ".sh")
            batchEuphoriaTestScript << testScript
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
        log.always("Remove test scripts")
        executionService.execute("rm ${batchEuphoriaTestScript}")
        executionService.executeLocal("rm ${batchEuphoriaTestScript}")
    }
}
