/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.sge

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedJobManager
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.TimeUnit
import groovy.util.slurpersupport.GPathResult

import java.time.Duration
import java.time.ZonedDateTime

/**
 * @author michael
 */
@groovy.transform.CompileStatic
class SGEJobManager extends GridEngineBasedJobManager<SGECommand> {

    SGEJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    @Override
    protected SGECommand createCommand(BEJob job) {
        return new SGECommand(this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id, job.tool?.getAbsolutePath() ?: job.getToolScript())
    }

    @Override
    String getQueryCommandForJobInfo() {
        return "qstat"
    }

    @Override
    String getQueryCommandForExtendedJobInfo() {
        return "qstat -xml -ext -f -j"
    }

    @Override
    ExtendedJobInfo parseGenericJobInfo(String command) {
        return null
    }

    @Override
    String parseJobID(String commandOutput) {
        if (!commandOutput.startsWith("Your job"))
            return null
        String id = commandOutput.split(StringConstants.SPLIT_WHITESPACE)[2]
        return id
    }

    @Override
    protected JobState parseJobState(String stateString) {
        //TODO: add all combinations, see http://www.softpanorama.org/HPC/Grid_engine/Queues/queue_states.shtml
        JobState js = JobState.UNKNOWN
        if (stateString == "r")
            js = JobState.RUNNING
        if (stateString == "hqw")
            js = JobState.HOLD
        if (stateString == "S")
            js = JobState.SUSPENDED
        if (stateString in ["qw", "T", "W"])
            js = JobState.QUEUED
        if (stateString in ["C", "E"]) {
            js = JobState.COMPLETED_UNKNOWN
        }

        return js;
    }

    @Override
    void createComputeParameter(ResourceSet resourceSet, LinkedHashMultimap<String, String> parameters) {
        parameters.put("-pe", "serial ${resourceSet.cores}")
    }

    void createQueueParameter(LinkedHashMultimap<String, String> parameters, String queue) {
        parameters.put('-q', queue)
    }

    @Override
    void createWalltimeParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put("-l", "h_rt=${TimeUnit.fromDuration(resourceSet.walltime).toHourString()}")
    }

    @Override
    void createMemoryParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put("-l", "h_rss=${resourceSet.getMem().toString(BufferUnit.M)}")
    }

    @Override
    void createStorageParameters(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
    }

    @Override
    String getJobIdVariable() {
        return "JOBID"
    }

    @Override
    String getJobNameVariable() {
        return "JOB_NAME"
    }

    @Override
    String getQueueVariable() {
        return null
    }

    @Override
    String getNodeFileVariable() {
        return null
    }

    @Override
    String getSubmitHostVariable() {
        return null
    }

    @Override
    String getSubmitDirectoryVariable() {
        return null
    }

    @Override
    String assembleJobInfoQueryCommand(List<BEJobID> jobIDs) {

        // SGE will output quite a bunch of information if you query by job. Thus said, just querying all jobs,
        // maybe filtered by the user, will possible result in less lines per query. E.g. a query with around
        // 8000 active jobs will result in approximately 830kByte, whereas a query with qstat -j ID will
        // result in 30 lines per job multiplied by (guessed) 40Byte per line = 1,2kByte per Job. We could overcome this
        // by putting in a filter with grep, but then again, the state is displayed differently than in the tabluar
        // qstat (e.g. scheduling info:            Job is in hold state), which makes it a little more difficult to use
        // => We'll stick to a simple "qstat (-u)" for now.
        String command = getQueryCommandForJobInfo()

        if (isTrackingOfUserJobsEnabled)
            command += " -u $userIDForQueries "

        return command
    }

    Map<BEJobID, ExtendedJobInfo> convertJobInfoMapToExtendedMap(Map<BEJobID, JobInfo> jobInfoMap) {
        return jobInfoMap.collectEntries {
            BEJobID jobID, JobInfo jobInfo ->
                [jobID, new ExtendedJobInfo(jobID, jobInfo.jobState)]
        } as Map<BEJobID, ExtendedJobInfo>
    }

    @Override
    Map<BEJobID, ExtendedJobInfo> queryExtendedJobInfo(List<BEJobID> jobIDs) {
        // Unfortunately, SGEs qstat does not show some information in extended mode, e.g.:
        // - The start time
        // - The job state (ok it sometimes displays something. But its some sort of message and it is not directly
        //                  attached to the job AND e.g. running is not shown at all
        // - Finish time
        // - Execution host
        // To overcome at least the missing job state, we call queryJobInfo() for this information first.

        Map<BEJobID, JobInfo> shortInfo = jobIDs == null ? queryAllJobInfo() : queryJobInfoByID(jobIDs)

        // If no jobs were found or all are of state unknown, we can actually just omit the rest.
        // This will make things a lot easier because:
        // - If there is NO job info in the result xml of the extended query, the result will be different and return
        //   invalid! (for my SGE tests, see the test files) xml code!
        // - If there is at least one job found, you will not see info about unknown jobs!
        // - So either preprocess the xml and correct it or check first, if we have info at
        //   all and omit the query then.
        if (shortInfo.size() == 0 || shortInfo.values().every { it.jobState == JobState.UNKNOWN })
            return convertJobInfoMapToExtendedMap(shortInfo)

        // Now we at least have some states, regardless of their existence in our list of job ids
        // Lets retrieve and parse our extended states.
        ExecutionResult er = executionService.execute(getQueryCommandForExtendedJobInfo())

        if (!er.successful)
            return convertJobInfoMapToExtendedMap(shortInfo)

        Map<BEJobID, ExtendedJobInfo> result = [:]

        // Finally we can parse our hopefully correct xml info
        def xml = new XmlSlurper().parseText(er.resultLines.join("\n"))

        xml["djob_info"]["element"].each {
            GPathResult element ->
                ExtendedJobInfo jobInfo = parseGPathJobInfoElement(element)
                jobInfo.jobState = shortInfo[jobInfo.jobID]?.jobState ?: JobState.UNKNOWN
                result[jobInfo.jobID] = jobInfo
        }

        // If a result is missing, it will be filled in later by the methods in BatchEuphoriaJobManager.
        return result
    }

    /**
     * The job state will NOT be parsed in this method! It is solely to be used as a helper for queryExtendedJobInfo
     */
    ExtendedJobInfo parseGPathJobInfoElement(GPathResult job) {
        ExtendedJobInfo jobInfo = new ExtendedJobInfo(new BEJobID(job["JB_job_number"] as String))

        /** Common */
        jobInfo.user = job["JB_owner"]
        jobInfo.userGroup = job["JB_group"]
        jobInfo.priority = job["JB_priority"]
        // jobInfo.submissionHost = job["JB_submit_host"] // Info is not available
        // jobInfo.server = job["JB_server"]              // Info is not available
        // jobInfo.umask = job["JB_umask"]                // Info is not available
        jobInfo.account = job["JB_account"]
        jobInfo.startCount = safelyCastToInteger(job["JB_restart"])

        /** Resources */
        // jobInfo.executionHosts = getExecutionHosts(job["JB_exec_host"]) // Info is not available
        // jobInfo.queue                                                   // Info is not available

        // Getting cores and nodes is a bit tricks. Nodes are not in the xml, cores via JB_pe_range
        String requestedCoresRaw = job["JB_pe_range"]["ranges"]["RN_min"]?.toString()
        Integer requestedCores = requestedCoresRaw ? requestedCoresRaw as Integer : null

        def requestedResourcesRaw = ((job["JB_hard_resource_list"]["qstat_l_requests"] as GPathResult).children() as List<GPathResult>)
                .findAll { it.name() == "qsat_l_requests" }.collectEntries {
            [it["CE_name"], it["CE_doubleval"] as Integer]
        }
        Long requestedMemoryRaw = requestedResourcesRaw["h_rss"] as Long
        BufferValue requestedMemory
        if (requestedMemoryRaw != null)
            requestedMemory = new BufferValue((Integer) (requestedMemoryRaw / 1024L), BufferUnit.k)

        Integer requestedWalltimeRaw = requestedResourcesRaw["h_rt"] as Integer
        Duration requestedWalltime = requestedWalltimeRaw ? Duration.ofSeconds(requestedWalltimeRaw) : null
        jobInfo.requestedResources = new ResourceSet(requestedMemory, requestedCores, null, requestedWalltime, null, null, null)

        Long usedWalltimeRaw = (extractStatisticsValue(job, "ru_wallclock") as Double) as Long
        Duration usedWalltime = null
        if (usedWalltime != null)
            Duration.ofSeconds(usedWalltimeRaw)
        Double usedMemoryRaw = extractStatisticsValue(job, "ru_maxrss") as Double
        BufferValue usedMemory = null
        if (usedMemoryRaw)
            usedMemory = new BufferValue((usedMemoryRaw / 1024) as Integer, BufferUnit.k)
        jobInfo.usedResources = new ResourceSet(usedMemory, requestedCores, null, usedWalltime, null, null, null)

        //        jobInfo.cpuTime   // acct_cpu??
        //        jobInfo.runTime = safelyCastToDuration(job["JB_total_runtime"]) // ?

        /** Status info */
        // State is not set here!
        jobInfo.exitCode = safelyCastToInteger(extractStatisticsValue(job, "exit_status"))

        /** Directories and files */
        boolean mergedLogs = job["JB_merge_stderr"] as Boolean
        def outPath = job["JB_stdout_path_list"]["path_list"]["PN_path"]
        jobInfo.logFile = outPath ? new File(outPath.toString()) : null
        if (!mergedLogs) {
            def errPath = job["JB_stderr_path_list"]["path_list"]["PN_path"]
            jobInfo.errorLogFile = errPath ? new File(errPath.toString()) : null
        }

        /** Timestamps */
        jobInfo.submitTime = extractAndParseTime(job, "submission_time")    // The time the job entered the current queue.
        jobInfo.startTime = extractAndParseTime(job, "start_time")          // The time the job was started.
        jobInfo.endTime = extractAndParseTime(job, "end_time")              // The time the job was completed.

        return jobInfo
    }

    ZonedDateTime extractAndParseTime(GPathResult job, String id) {
        safelyParseTime((extractStatisticsValue(job, id) as Double) as Integer)
    }

    Object extractStatisticsValue(GPathResult job, String id) {
        GPathResult usageList = job["JB_ja_tasks"]["ulong_sublist"]["JAT_scaled_usage_list"] as GPathResult
        GPathResult times = usageList.children().findAll { GPathResult it -> it.name() == "scaled" }
        def entry = times.collectEntries {
            [it["UA_name"]?.toString(), it["UA_value"].toString()]
        }[id]
        entry
    }
}
