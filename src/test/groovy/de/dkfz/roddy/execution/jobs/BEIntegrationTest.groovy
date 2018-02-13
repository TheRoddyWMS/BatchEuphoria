/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.AvailableClusterSystems
import de.dkfz.roddy.BEException
import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.RestExecutionService
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import groovy.transform.CompileStatic
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import java.time.Duration

/**
 *
 * Only the tests are executed for which the cluster configuration are set in the "integrationTest.properties"
 *
 */
@CompileStatic
class BEIntegrationTest {
    static Properties properties
    static TestExecutionService executionService
    static RestExecutionService restExecutionService
    static String testScript = "ls"
    static File batchEuphoriaTestScript
    static JobLog logFile
    static ResourceSet resourceSet = new ResourceSet(new BufferValue(10, BufferUnit.m), 1, 1, Duration.ofMinutes(1), null, null, null)


    @BeforeClass
    static void readProperties() {
        properties = new Properties()
        File propertiesFile = new File('integrationTest.properties')
        propertiesFile.withInputStream {
            properties.load(it)
        }

        if(properties."logpath" != "")
            logFile = JobLog.toOneFile(new File(properties."logpath" as String))
        else
            logFile = JobLog.none()
    }

    static BatchEuphoriaJobManager runTestsFor(AvailableClusterSystems option, BEExecutionService executionService) {
        return option.loadClass().getDeclaredConstructor(BEExecutionService, JobManagerOptions)
                .newInstance(executionService,
                JobManagerOptions.create()
                        .setCreateDaemon(false)
                        .setTrackUserJobsOnly(true)
                        .build()
        ) as BatchEuphoriaJobManager
    }


    static void testJobWithPipedScript(BatchEuphoriaJobManager jobManager) {
        BEJob testJobWithPipedScript = new BEJob(null, "batchEuphoriaTestJob", null, testScript, null, resourceSet, null, ["a": "value"], jobManager, logFile, null)
        jobTest(jobManager, [testJobWithPipedScript])
    }

    static void testJobWithFile(BatchEuphoriaJobManager jobManager) {
        BEJob testJobWithFile = new BEJob(null, "batchEuphoriaTestJob", batchEuphoriaTestScript, null, null, resourceSet, null, ["a": "value"], jobManager, logFile, null)
        jobTest(jobManager, [testJobWithFile])
    }

    static void testMultipleJobsWithPipedScript(BatchEuphoriaJobManager jobManager) {

        BEJob testParent = new BEJob(null, "batchEuphoriaTestJob_Parent", null, testScript, null, resourceSet, null, ["a": "value"], jobManager, logFile, null)
        BEJob testJobChild1 = new BEJob(null, "batchEuphoriaTestJob_Child1", null, testScript, null, resourceSet, [testParent], ["a": "value"], jobManager, logFile, null)
        BEJob testJobChild2 = new BEJob(null, "batchEuphoriaTestJob_Child2", null, testScript, null, resourceSet, [testParent, testJobChild1], ["a": "value"], jobManager, logFile, null)
        jobTest(jobManager, [testParent, testJobChild1, testJobChild2])
    }

    static void testMultipleJobsWithFile(BatchEuphoriaJobManager jobManager) {

        BEJob testParent = new BEJob(null, "batchEuphoriaTestJob_Parent", batchEuphoriaTestScript, null, null, resourceSet, null, ["a": "value"], jobManager, logFile, null)
        BEJob testJobChild1 = new BEJob(null, "batchEuphoriaTestJob_Child1", batchEuphoriaTestScript, null, null, resourceSet, [testParent], ["a": "value"], jobManager, logFile, null)
        BEJob testJobChild2 = new BEJob(null, "batchEuphoriaTestJob_Child2", batchEuphoriaTestScript, null, null, resourceSet, [testParent, testJobChild1], ["a": "value"], jobManager, logFile, null)
        jobTest(jobManager, [testParent, testJobChild1, testJobChild2])
    }


    static void jobTest(BatchEuphoriaJobManager jobManager, List<BEJob> testJobs) {
        int maxSleep = 5

        testJobs.each {
            def jr = jobManager.submitJob(it)
            println "Job ID: ${jr.jobID.id}"
        }

        if (jobManager.isHoldJobsEnabled()) {

            ensureProperJobStates(maxSleep, testJobs, [JobState.HOLD], jobManager)

            jobManager.startHeldJobs(testJobs)
            // Wait for some seconds and see, if the status changes from HOLD to queued or running and from queued to running
            // The queued to running check can take a lot more time. Also the default update time for queries to the job system
            // is too long for tests. We force updates everytime we run queryJobStatus
            ensureProperJobStates(maxSleep, testJobs, [JobState.QUEUED, JobState.RUNNING], jobManager)
        } else {
            ensureProperJobStates(maxSleep, testJobs, [JobState.QUEUED, JobState.HOLD, JobState.RUNNING], jobManager)
        }

        jobManager.queryExtendedJobState(testJobs)

        ensureProperJobStates(maxSleep, testJobs, [JobState.COMPLETED_SUCCESSFUL], jobManager)

    }


    static void ensureProperJobStates(int maxSleep, List<BEJob> jobList, List<JobState> listOfStatesToCheck, BatchEuphoriaJobManager jobManager) {
        int increments = 8
        int sleep = maxSleep * increments  // 125ms increments, count from 5s to 0 seconds.
        boolean allJobsInCorrectState = false
        List<JobState> lastStates = []
        while (sleep > 0 && !allJobsInCorrectState) {
            lastStates.clear()
            def status = jobManager.queryJobStatus(jobList, true)
            allJobsInCorrectState = true
            for (BEJob job in jobList) {
                allJobsInCorrectState &= listOfStatesToCheck.contains(status[job])
                lastStates << status[job]
            }
            if (!allJobsInCorrectState) {
                assert status.values().join(" ").find(JobState.FAILED.name()) != JobState.FAILED.name()
                sleep--
            }
        }


        if (!allJobsInCorrectState && !lastStates.join(" ").find(JobState.RUNNING.name()))
            new BEException("Not all jobs ${jobList.collect { it.jobID }.join(" ")} were in the proper state: " +
                    "[${listOfStatesToCheck.join(" ")}], got last states [${lastStates.join(" ")}]. " +
                    "Make sure, that your job system is working properly.")
    }


    static void prepareTestScript(){
        batchEuphoriaTestScript = new File(properties."remoteToolPath" as String)
        executionService.execute("mkdir -p ${batchEuphoriaTestScript.parentFile}")
        if(properties."testscript" != "")
            testScript << properties."testscript"

        executionService.execute("echo ${testScript} > ${batchEuphoriaTestScript}")
        executionService.execute("chmod +x ${batchEuphoriaTestScript}")
    }

    @Test
    void testLsfMultipleJobsWithPipedScript() {
        if (properties."lsf.host" != "" && properties."lsf.user" != "") {
            executionService = new TestExecutionService(properties."lsf.account" as String, properties."lsf.host" as String)
            testMultipleJobsWithPipedScript(runTestsFor(AvailableClusterSystems.lsf, executionService))
        }
    }

    @Test
    void testLsfMultipleJobsWithFile() {
        if (properties."lsf.host" != "" && properties."lsf.user" != "" && properties.remoteToolPath != "") {
            executionService = new TestExecutionService(properties."lsf.account" as String, properties."lsf.host" as String)
            prepareTestScript()
            testMultipleJobsWithFile(runTestsFor(AvailableClusterSystems.lsf, executionService))
        }
    }

    @Test
    void testLsfJobWithFile() {
        if (properties."lsf.host" != "" && properties."lsf.user" != "" && properties.remoteToolPath != "") {
            executionService = new TestExecutionService(properties."lsf.account" as String, properties."lsf.host" as String)
            prepareTestScript()
            testJobWithFile(runTestsFor(AvailableClusterSystems.lsf, executionService))
        }
    }

    @Test
    void testLsfJobWithPipedScript() {
        if (properties."lsf.host" != "" && properties."lsf.user" != "") {
            executionService = new TestExecutionService(properties."lsf.account" as String, properties."lsf.host" as String)
            testJobWithPipedScript(runTestsFor(AvailableClusterSystems.lsf, executionService))
        }
    }

    @Test
    void testPbsMultipleJobsWithPipedScript() {
        if (properties."pbs.host" != "" && properties."pbs.user" != "") {
            executionService = new TestExecutionService(properties."pbs.account" as String, properties."pbs.host" as String)
            testMultipleJobsWithPipedScript(runTestsFor(AvailableClusterSystems.pbs, executionService))
        }
    }

    @Test
    void testPbsMultipleJobsWithFile() {
        if (properties."pbs.host" != "" && properties."pbs.user" != "" && properties.remoteToolPath != "") {
            executionService = new TestExecutionService(properties."pbs.account" as String, properties."pbs.host" as String)
            prepareTestScript()
            testMultipleJobsWithFile(runTestsFor(AvailableClusterSystems.pbs, executionService))
        }
    }

    @Test
    void testPbsJobWithFile() {
        if (properties."pbs.host" != "" && properties."pbs.user" != "" && properties.remoteToolPath != "") {
            executionService = new TestExecutionService(properties."pbs.account" as String, properties."pbs.host" as String)
            prepareTestScript()
            testJobWithFile(runTestsFor(AvailableClusterSystems.pbs, executionService))
        }
    }

    @Test
    void testPbsJobWithPipedScript() {
        if (properties."pbs.host" != "" && properties."pbs.user" != "") {
            executionService = new TestExecutionService(properties."pbs.account" as String, properties."pbs.host" as String)
            testJobWithPipedScript(runTestsFor(AvailableClusterSystems.pbs, executionService))
        }
    }


    @Test
    void testLsfRestMultipleJobsWithPipedScript() {
        if (![properties."lsf.rest.host" as String, properties."lsf.rest.account" as String, properties."lsf.rest.password" as String]*.isEmpty()) {
            restExecutionService = new RestExecutionService(properties."lsf.rest.host" as String, properties."lsf.rest.account" as String, properties."lsf.rest.password" as String)
            testMultipleJobsWithPipedScript(runTestsFor(AvailableClusterSystems.lsfrest, restExecutionService))
        }
    }

    @Test
    void testLsfRestMultipleJobsWithFile() {
        if (![properties."lsf.rest.host" as String, properties."lsf.rest.account" as String, properties."lsf.rest.password" as String, properties."lsf.account" as String, properties."lsf.host" as String]*.isEmpty() && properties.remoteToolPath != "") {
            restExecutionService = new RestExecutionService(properties."lsf.rest.host" as String, properties."lsf.rest.account" as String, properties."lsf.rest.password" as String)
            executionService = new TestExecutionService(properties."lsf.account" as String, properties."lsf.host" as String)
            prepareTestScript()
            testMultipleJobsWithFile(runTestsFor(AvailableClusterSystems.lsfrest, restExecutionService))
        }
    }

    @Test
    void testLsfRestJobWithFile() {
        if (![properties."lsf.rest.host" as String, properties."lsf.rest.account" as String, properties."lsf.rest.password" as String, properties."lsf.account" as String, properties."lsf.host" as String]*.isEmpty() && properties.remoteToolPath != "") {
            restExecutionService = new RestExecutionService(properties."lsf.rest.host" as String, properties."lsf.rest.account" as String, properties."lsf.rest.password" as String)
            executionService = new TestExecutionService(properties."lsf.account" as String, properties."lsf.host" as String)
            prepareTestScript()
            testJobWithFile(runTestsFor(AvailableClusterSystems.lsfrest, restExecutionService))

        }
    }

    @Test
    void testLsfRestJobWithPipedScript() {
        if (![properties."lsf.rest.host" as String, properties."lsf.rest.account" as String, properties."lsf.rest.password" as String]*.isEmpty()) {
            restExecutionService = new RestExecutionService(properties."lsf.rest.host" as String, properties."lsf.rest.account" as String, properties."lsf.rest.password" as String)
            testJobWithPipedScript(runTestsFor(AvailableClusterSystems.lsfrest, restExecutionService))
        }
    }

    @Test
    void testSgeMultipleJobsWithPipedScript() {
        if (properties."sge.host" != "" && properties."sge.user" != "") {
            executionService = new TestExecutionService(properties."sge.account" as String, properties."sge.host" as String)
            testMultipleJobsWithPipedScript(runTestsFor(AvailableClusterSystems.sge, executionService))
        }
    }

    @Test
    void testSgeMultipleJobsWithFile() {
        if (properties."sge.host" != "" && properties."sge.user" != "" && properties.remoteToolPath != "") {
            executionService = new TestExecutionService(properties."sge.account" as String, properties."sge.host" as String)
            prepareTestScript()
            testMultipleJobsWithFile(runTestsFor(AvailableClusterSystems.sge, executionService))
        }
    }

    @Test
    void testSgeJobWithFile() {
        if (properties."sge.host" != "" && properties."sge.user" != "" && properties.remoteToolPath != "") {
            executionService = new TestExecutionService(properties."sge.account" as String, properties."sge.host" as String)
            prepareTestScript()
            testJobWithFile(runTestsFor(AvailableClusterSystems.sge, executionService))
        }
    }

    @Test
    void testSgeJobWithPipedScript() {
        if (properties."sge.host" != "" && properties."sge.user" != "") {
            executionService = new TestExecutionService(properties."sge.account" as String, properties."sge.host" as String)
            testJobWithPipedScript(runTestsFor(AvailableClusterSystems.sge, executionService))
        }
    }


    @Test
    void testSlurmMultipleJobsWithPipedScript() {
        if (properties."slurm.host" != "" && properties."slurm.user" != "") {
            executionService = new TestExecutionService(properties."slurm.account" as String, properties."slurm.host" as String)
            testMultipleJobsWithPipedScript(runTestsFor(AvailableClusterSystems.slurm, executionService))
        }
    }

    @Test
    void testSlurmMultipleJobsWithFile() {
        if (properties."slurm.host" != "" && properties."slurm.user" != "" && properties.remoteToolPath != "") {
            executionService = new TestExecutionService(properties."slurm.account" as String, properties."slurm.host" as String)
            prepareTestScript()
            testMultipleJobsWithFile(runTestsFor(AvailableClusterSystems.slurm, executionService))

        }
    }

    @Test
    void testSlurmJobWithFile() {
        if (properties."slurm.host" != "" && properties."slurm.user" != "" && properties.remoteToolPath != "") {
            executionService = new TestExecutionService(properties."slurm.account" as String, properties."slurm.host" as String)
            prepareTestScript()
            testJobWithFile(runTestsFor(AvailableClusterSystems.slurm, executionService))

        }
    }

    @Test
    void testSlurmJobWithPipedScript() {
        if (properties."slurm.host" != "" && properties."slurm.user" != "") {
            executionService = new TestExecutionService(properties."slurm.account" as String, properties."slurm.host" as String)
            testJobWithPipedScript(runTestsFor(AvailableClusterSystems.slurm, executionService))
        }
    }

}
