/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedJobManager
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.PackageScope
import groovy.util.slurpersupport.GPathResult

/**
 * @author michael
 */
@groovy.transform.CompileStatic
class PBSJobManager extends GridEngineBasedJobManager<PBSCommand> {

    PBSJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    @Override
    protected PBSCommand createCommand(BEJob job) {
        return new PBSCommand(this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id, job.tool?.getAbsolutePath() ?: job.getToolScript())
    }

    @Override
    ExtendedJobInfo parseGenericJobInfo(String commandString) {
        return new PBSCommandParser(commandString).toGenericJobInfo()
    }

    /**
     * Return the position of the status string within a stat result line. This changes if -u USERNAME is used!
     *
     * @return
     */
    protected int getColumnOfJobState() {
        if (isTrackingOfUserJobsEnabled)
            return 9
        return 4
    }

    @Override
    String parseJobID(String commandOutput) {
        return commandOutput
    }

    @Override
    protected JobState parseJobState(String stateString) {
        // http://docs.adaptivecomputing.com/torque/6-1-0/adminGuide/help.htm#topics/torque/commands/qstat.htm
        JobState js = JobState.UNKNOWN
        if (stateString == "R")
            js = JobState.RUNNING
        if (stateString == "H")
            js = JobState.HOLD
        if (stateString == "S")
            js = JobState.SUSPENDED
        if (stateString in ["Q", "T", "W"])
            js = JobState.QUEUED
        if (stateString in ["C", "E"]) {
            js = JobState.COMPLETED_UNKNOWN
        }

        return js;
    }

    @Override
    List<String> getEnvironmentVariableGlobs() {
        return Collections.unmodifiableList(["PBS_*"])
    }

    @Override
    String getQueryCommandForJobInfo() {
        return "qstat -t"
    }

    @Override
    String getQueryCommandForExtendedJobInfo() {
        return "qstat -x -f"
    }

    @Override
    void createComputeParameter(ResourceSet resourceSet, LinkedHashMultimap<String, String> parameters) {
        int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
        int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1
        // Currently not active
        String enforceSubmissionNodes = ''
        if (!enforceSubmissionNodes) {
            String pVal = 'nodes=' << nodes << ':ppn=' << cores
            if (resourceSet.isAdditionalNodeFlagSet()) {
                pVal << ':' << resourceSet.getAdditionalNodeFlag()
            }
            parameters.put("-l", pVal)
        } else {
            String[] nodesArr = enforceSubmissionNodes.split(StringConstants.SPLIT_SEMICOLON)
            nodesArr.each {
                String node ->
                    parameters.put('-l', 'nodes=' + node + ':ppn=' + resourceSet.getCores())
            }
        }
    }

    void createQueueParameter(LinkedHashMultimap<String, String> parameters, String queue) {
        parameters.put('-q', queue)
    }

    @Override
    void createWalltimeParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put('-l', 'walltime=' + TimeUnit.fromDuration(resourceSet.walltime))
    }

    @Override
    void createMemoryParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put('-l', 'mem=' + resourceSet.getMem().toString(BufferUnit.M))
    }

    @Override
    void createStorageParameters(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
    }

    @Override
    String getJobIdVariable() {
        return "PBS_JOBID"
    }

    @Override
    String getJobNameVariable() {
        return "PBS_JOBNAME"
    }

    @Override
    String getQueueVariable() {
        return 'PBS_QUEUE'
    }

    @Override
    String getNodeFileVariable() {
        return "PBS_NODEFILE"
    }

    @Override
    String getSubmitHostVariable() {
        return "PBS_O_HOST"
    }

    @Override
    String getSubmitDirectoryVariable() {
        return "PBS_O_WORKDIR"
    }


    @Override
    Map<BEJobID, ExtendedJobInfo> queryExtendedJobInfo(List<BEJobID> jobIds) {
        Map<BEJobID, ExtendedJobInfo> queriedExtendedStates = [:]
        String qStatCommand = getQueryCommandForExtendedJobInfo()
        qStatCommand += " " + jobIds.collect { it }.join(" ")
        // For the reviewer: This does not make sense right? We have a list of given ids which we want to
        // query anyway.
        //        if (isTrackingOfUserJobsEnabled)
        //            qStatCommand += " -u $userIDForQueries "

        ExecutionResult er = executionService.execute(qStatCommand.toString())
        if (er != null && er.successful) {
            GPathResult parsedJobs = new XmlSlurper().parseText(er.resultLines.join("\n"))
            for (BEJobID jobID : jobIds) {
                def jobInfo = parsedJobs.children().find { it["Job_Id"].toString().startsWith("${jobID.id}.") }
                queriedExtendedStates[jobID] = this.processQstatOutputFromXMLNodeChild(jobInfo)
            }
        } else {
            throw new BEException("Extended job states couldn't be retrieved. \n Returned status code:${er.exitCode} \n ${qStatCommand.toString()} \n\t result:${er.resultLines.join("\n\t")}")
        }
        return queriedExtendedStates
    }

    /**
     * Reads the qstat output and creates ExtendedJobInfo objects
     * @param resultLines - Input of ExecutionResult object
     * @return map with jobid as key
     */
    @PackageScope
    ExtendedJobInfo processQstatOutputFromXMLNodeChild(GPathResult job) {
        String jobIdRaw = job["Job_Id"] as String
        BEJobID jobID = toJobID(jobIdRaw)

        List<String> jobDependencies = getJobDependencies(job["depend"])
        ExtendedJobInfo jobInfo = new ExtendedJobInfo(job["Job_Name"] as String, null, jobID, null, jobDependencies)

        /** Common */
        jobInfo.user = (job["Job_Owner"] as String)?.split(StringConstants.SPLIT_AT)[0]
        jobInfo.userGroup = job["egroup"]
        jobInfo.priority = job["Priority"]
        jobInfo.submissionHost = job["submit_host"]
        jobInfo.server = job["server"]
        jobInfo.umask = job["umask"]
        jobInfo.account = job["Account_Name"]
        jobInfo.startCount = safelyCastToInteger(job["start_count"])

        /** Resources
         * Note: Both nodes and nodect are deprecated values
         *       nodect is the number of requested nodes and of type integer
         */
        jobInfo.executionHosts = getExecutionHosts(job["exec_host"])
        String queue = job["queue"] as String

        def requestedResources = job["Resource_List"]
        String resourcesListNodes = requestedResources["nodes"]
        Integer requestedNodes = safelyCastToInteger(requestedResources["nodect"])
        Integer requestedCores = safelyCastToInteger(resourcesListNodes?.find("ppn=.*")?.find(/(\d+)/))
        String requestedAdditionalNodeFlag = withCaughtAndLoggedException { resourcesListNodes?.find(/(\d+):(\.*)/) { fullMatch, nCores, feature -> return feature } }
        TimeUnit requestedWalltime = safelyCastToTimeUnit(requestedResources["walltime"])
        BufferValue requestedMemory = safelyCastToBufferValue(requestedResources["mem"])

        def usedResources = job["resources_used"]
        TimeUnit usedWalltime = safelyCastToTimeUnit(usedResources["walltime"])
        BufferValue usedMemory = safelyCastToBufferValue(usedResources["mem"])

        jobInfo.requestedResources = new ResourceSet(null, requestedMemory, requestedCores, requestedNodes, requestedWalltime, null, queue, requestedAdditionalNodeFlag)
        jobInfo.usedResources = new ResourceSet(null, usedMemory, null, null, usedWalltime, null, queue, null)
        jobInfo.rawResourceRequest = job["submit_args"]
        jobInfo.runTime = safelyCastToDuration(job["total_runtime"])
        jobInfo.cpuTime = safelyParseColonSeparatedDuration(usedResources["cput"])

        /** Status info */
        jobInfo.jobState = parseJobState(job["job_state"] as String)
        jobInfo.exitCode = safelyCastToInteger(job["exit_status"])

        /** Directories and files */
        jobInfo.logFile = safelyGetQstatFile(job["Output_Path"] as String, jobIdRaw)
        jobInfo.errorLogFile = safelyGetQstatFile(job["Error_Path"] as String, jobIdRaw)

        /** Timestamps */
        jobInfo.submitTime = safelyParseTime(job["qtime"])      // The time that the job entered the current queue.
        jobInfo.startTime = safelyParseTime(job["start_time"])  // The timepoint the job was started.
        jobInfo.endTime = safelyParseTime(job["comp_time"])     // The timepoint the job was completed.
        jobInfo.eligibleTime = safelyParseTime(job["etime"])    // The time that the job became eligible to run, i.e. in a queued state while residing in an execution queue.
        // http://docs.adaptivecomputing.com/torque/4-2-7/Content/topics/9-accounting/accountingRecords.htm
        // Left is ctime: The the time the job was created

        return jobInfo
    }


}
