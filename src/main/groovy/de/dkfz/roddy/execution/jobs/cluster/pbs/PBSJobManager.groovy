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
import de.dkfz.roddy.execution.jobs.cluster.ClusterJobManager
import de.dkfz.roddy.tools.*

import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.regex.Matcher

/**
 * A job submission implementation for standard PBS systems.
 *
 * @author michael
 */
@groovy.transform.CompileStatic
class PBSJobManager extends ClusterJobManager<PBSCommand> {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(PBSJobManager)

    private static final String PBS_COMMAND_QUERY_STATES = "qstat -t"
    private static final String PBS_COMMAND_QUERY_STATES_FULL = "qstat -f"
    private static final String WITH_DELIMITER = '(?=(%1$s))'

    PBSJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
        /**
         * General or specific todos for BatchEuphoriaJobManager and PBSJobManager
         */
    }

    @Override
    protected PBSCommand createCommand(BEJob job) {
        return new PBSCommand(this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id, job.tool?.getAbsolutePath() ?: job.getToolScript())
    }

    /**
     * For PBS, we enable hold jobs by default.
     * If it is not enabled, we might run into the problem, that job dependencies cannot be
     * resolved early enough due to timing problems.
     * @return
     */
    @Override
    boolean getDefaultForHoldJobsEnabled() { return true }

    @Override
    protected ExecutionResult executeStartHeldJobs(List<BEJobID> jobIDs) {
        String command = "qrls ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String commandString) {
        return new PBSCommandParser(commandString).toGenericJobInfo();
    }

    private String getQueryCommand() {
        return PBS_COMMAND_QUERY_STATES
    }

    @Override
    protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs) {
        StringBuilder queryCommand = new StringBuilder(getQueryCommand())

        if (jobIDs && jobIDs.size() < 10) {
            queryCommand << " "<< jobIDs*.id.join(" ")
        }

        if (isTrackingOfUserJobsEnabled)
            queryCommand << " -u $userIDForQueries "

        ExecutionResult er = executionService.execute(queryCommand.toString())
        List<String> resultLines = er.resultLines

        Map<BEJobID, JobState> result = [:]

        if (!er.successful) {
            if (strictMode) // Do not pull this into the outer if! The else branch needs to be executed if er.successful is true
                throw new BEException("The execution of ${queryCommand} failed.", null)
        } else {
            if (resultLines.size() > 2) {

                for (String line : resultLines) {
                    line = line.trim()
                    if (line.length() == 0) continue
                    if (!RoddyConversionHelperMethods.isInteger(line.substring(0, 1)))
                        continue //Filter out lines which have been missed which do not start with a number.

                    //TODO Put to a common class, is used multiple times.
                    line = line.replaceAll("\\s+", " ").trim()       //Replace multi white space with single whitespace
                    String[] split = line.split(" ")
                    final int ID = getPositionOfJobID()
                    final int JOBSTATE = getPositionOfJobState()
                    logger.info(["QStat BEJob line: " + line,
                                 "	Entry in arr[" + ID + "]: " + split[ID],
                                 "    Entry in arr[" + JOBSTATE + "]: " + split[JOBSTATE]].join("\n"))

                    BEJobID jobID = new BEJobID(split[ID])

                    JobState js = parseJobState(split[JOBSTATE])
                    result.put(jobID, js)
                    logger.info("   Extracted jobState: " + js.toString())
                }
            }
        }
        return result
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

    protected int getPositionOfJobID() {
        return 0
    }

    /**
     * Return the position of the status string within a stat result line. This changes if -u USERNAME is used!
     *
     * @return
     */
    protected int getPositionOfJobState() {
        if (isTrackingOfUserJobsEnabled)
            return 9
        return 4
    }

    @Override
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates
        String qStatCommand = PBS_COMMAND_QUERY_STATES_FULL
        if (isTrackingOfUserJobsEnabled)
            qStatCommand += " -u $userIDForQueries "

        qStatCommand += jobIds.collect { it }.join(" ")

        ExecutionResult er
        try {
            er = executionService.execute(qStatCommand.toString())
        } catch (Exception exp) {
            logger.severe("Could not execute qStat command", exp)
        }

        if (er != null && er.successful) {
            queriedExtendedStates = this.processQstatOutput(er.resultLines)
        }else{
            logger.postAlwaysInfo("Extended job states couldn't be retrieved. \n Returned status code:${er.exitCode} \n result:${er.resultLines}")
            throw new BEException("Extended job states couldn't be retrieved. \n Returned status code:${er.exitCode} \n result:${er.resultLines}")
        }
        return queriedExtendedStates
    }

    @Override
    ExecutionResult executeKillJobs(List<BEJobID> jobIDs) {
        String command = "qdel ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    @Override
    String parseJobID(String commandOutput) {
        return commandOutput
    }

    @Override
    String getSubmissionCommand() {
        return "qsub"
    }

    /**
     * Reads qstat output
     * @param qstatOutput
     * @return output of qstat in a map with jobid as key
     */
    private Map<String, Map<String, String>> readQstatOutput(String qstatOutput) {
        return qstatOutput.split(String.format(WITH_DELIMITER, "\n\nJob Id: ")).collectEntries {
            Matcher matcher = it =~ /^\s*Job Id: (?<jobId>\d+)\..*\n/
            def result = new HashMap()
            if (matcher) {
                result[matcher.group("jobId")] = it
            }
            result
        }.collectEntries { jobId, value ->
            // join multi-line values
            value = ((String) value).replaceAll("\n\t", "")
            [(jobId): value]
        }.collectEntries { jobId, value ->
            Map<String, String> p = ((String) value).readLines().
                    findAll { it.startsWith("    ") && it.contains(" = ") }.
                    collectEntries {
                        String[] parts = it.split(" = ")
                        new MapEntry(parts.head().replaceAll(/^ {4}/, ""), parts.tail().join(' '))
                    }
            [(jobId): p]
        } as Map<String, Map<String, String>>
    }

    private static LocalDateTime parseTime(String str) {
        def pbsDatePattern = DateTimeFormatter.ofPattern("EEE MMM ppd HH:mm:ss yyyy").withLocale(Locale.ENGLISH)
        return LocalDateTime.parse(str, pbsDatePattern)
    }

    /**
     * Reads the qstat output and creates GenericJobInfo objects
     * @param resultLines - Input of ExecutionResult object
     * @return map with jobid as key
     */
    private Map<BEJobID, GenericJobInfo> processQstatOutput(List<String> resultLines) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates = [:]
        Map<String, Map<String, String>> qstatReaderResult = this.readQstatOutput(resultLines.join("\n"))

        qstatReaderResult.each { it ->
                Map<String, String> jobResult = it.getValue()
            GenericJobInfo gj = new GenericJobInfo(jobResult["Job_Name"], null, it.getKey(), null, jobResult["depend"] ? jobResult["depend"].find("afterok.*")?.findAll(/(\d+).(\w+)/) { fullMatch, beforeDot, afterDot -> return beforeDot } : null)

            BufferValue mem = null
            Integer cores
            Integer nodes
            TimeUnit walltime = null
            String additionalNodeFlag

            if (jobResult["Resource_List.mem"])
                mem = catchExceptionAndLog { new BufferValue(Integer.valueOf(jobResult["Resource_List.mem"].find(/(\d+)/)), BufferUnit.valueOf(jobResult["Resource_List.mem"][-2])) }
            if (jobResult["Resource_List.nodect"])
                nodes = catchExceptionAndLog { Integer.valueOf(jobResult["Resource_List.nodect"]) }
            if (jobResult["Resource_List.nodes"])
                cores = catchExceptionAndLog { Integer.valueOf(jobResult["Resource_List.nodes"].find("ppn=.*").find(/(\d+)/)) }
            if (jobResult["Resource_List.nodes"])
                additionalNodeFlag = catchExceptionAndLog { jobResult["Resource_List.nodes"].find(/(\d+):(\.*)/) { fullMatch, nCores, feature -> return feature } }
            if (jobResult["Resource_List.walltime"])
                walltime = catchExceptionAndLog { new TimeUnit(jobResult["Resource_List.walltime"]) }

            BufferValue usedMem = null
            TimeUnit usedWalltime = null
            if (jobResult["resources_used.mem"])
                catchExceptionAndLog { usedMem = new BufferValue(Integer.valueOf(jobResult["resources_used.mem"].find(/(\d+)/)), BufferUnit.valueOf(jobResult["resources_used.mem"][-2]))  }
            if (jobResult["resources_used.walltime"])
                catchExceptionAndLog { usedWalltime = new TimeUnit(jobResult["resources_used.walltime"]) }

            gj.askedResources = new ResourceSet(null, mem, cores, nodes, walltime, null, jobResult["queue"], additionalNodeFlag)
            gj.setUsedResources(new ResourceSet(null, usedMem, null, null, usedWalltime, null, jobResult["queue"], null))

            gj.logFile = getQstatFile(jobResult["Output_Path"])
            gj.errorLogFile = getQstatFile(jobResult["Error_Path"])
            gj.user = jobResult["euser"]
            gj.executionHosts = jobResult["exec_host"]
            gj.submissionHost = jobResult["submit_host"]
            gj.priority = jobResult["Priority"]
            gj.userGroup = jobResult["egroup"]
            gj.resourceReq = jobResult["submit_args"]
            gj.runTime = jobResult["total_runtime"] ? catchExceptionAndLog { Duration.ofSeconds(Math.round(Double.parseDouble(jobResult["total_runtime"])), 0) } : null
            gj.cpuTime = jobResult["resources_used.cput"] ? catchExceptionAndLog { parseColonSeparatedHHMMSSDuration(jobResult["resources_used.cput"]) } : null
            gj.server = jobResult["server"]
            gj.umask = jobResult["umask"]
            gj.jobState = parseJobState(jobResult["job_state"])
            gj.exitCode = jobResult["exit_status"] ? catchExceptionAndLog { Integer.valueOf(jobResult["exit_status"]) } : null
            gj.account = jobResult["Account_Name"]
            gj.startCount = jobResult["start_count"] ? catchExceptionAndLog { Integer.valueOf(jobResult["start_count"]) } : null

            if (jobResult["qtime"]) // The time that the job entered the current queue.
                    catchExceptionAndLog { gj.setSubmitTime(parseTime(jobResult["qtime"])) }
                if (jobResult["start_time"]) // The timepoint the job was started.
                    catchExceptionAndLog { gj.setStartTime(parseTime(jobResult["start_time"])) }
                if (jobResult["comp_time"])  // The timepoint the job was completed.
                    catchExceptionAndLog { gj.setEndTime(parseTime(jobResult["comp_time"])) }
                if (jobResult["etime"])  // The time that the job became eligible to run, i.e. in a queued state while residing in an execution queue.
                    catchExceptionAndLog { gj.setEligibleTime(parseTime(jobResult["etime"])) }

                queriedExtendedStates.put(new BEJobID(it.getKey()), gj)
        }

        return queriedExtendedStates
    }

    private static File getQstatFile(String s) {
        if (!s) {
            return null
        } else if (s.startsWith("/")) {
            return new File(s)
        } else if (s =~ /^[\w-]+:\//) {
            return new File(s.replaceAll(/^[\w-]+:/, ""))
        } else {
            return null
        }
    }

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

    void createDefaultManagerParameters(LinkedHashMultimap<String, String> parameters) {

    }

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

    /**
     * Valid for GE, PBS, LSF
     * @param parameters
     * @param queue
     * @return
     */
    void createQueueParameter(LinkedHashMultimap<String, String> parameters, String queue) {
        parameters['-q'] = queue
    }

    /**
     * Valid only for PBS
     * @param parameters
     * @param resourceSet
     * @return
     */
    void createWalltimeParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put('-l', 'walltime=' + TimeUnit.fromDuration(resourceSet.walltime))
    }

    void createMemoryParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put('-l', 'mem=' + resourceSet.getMem().toString(BufferUnit.M))
    }

    void createStorageParameters(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
    }

}
