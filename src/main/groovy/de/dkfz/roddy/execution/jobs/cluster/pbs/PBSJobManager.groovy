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
import groovy.util.slurpersupport.GPathResult

import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
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
    private static final String PBS_COMMAND_QUERY_STATES_FULL = "qstat -x -f "
    private static final String PBS_COMMAND_DELETE_JOBS = "qdel"
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
        return catchExceptionAndLog {Instant.ofEpochSecond(Long.valueOf(str)).atZone(ZoneId.systemDefault()).toLocalDateTime()}
    }

    /**
     * Reads the qstat output and creates GenericJobInfo objects
     * @param resultLines - Input of ExecutionResult object
     * @return map with jobid as key
     */
    private Map<BEJobID, GenericJobInfo> processQstatOutput(List<String> resultLines) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates = [:]
        if(resultLines.join("\n").isEmpty()){
            return [:]
        }
        List<GPathResult> parsedJobs = resultLines.collect {new XmlSlurper().parseText(it)}
        parsedJobs.each { it ->
            GenericJobInfo gj = new GenericJobInfo(it["Job_Name"] as String, null, it["Job_Id"] as String, null, it["depend"] ? (it["depend"] as  String).find("afterok.*")?.findAll(/(\d+).(\w+)/) { fullMatch, beforeDot, afterDot -> return beforeDot } : null)

            BufferValue mem = null
            Integer cores
            Integer nodes
            TimeUnit walltime = null
            String additionalNodeFlag

            if (it["Resource_List"]["mem"])
                mem = catchExceptionAndLog { new BufferValue(Integer.valueOf((it["Resource_List"]["mem"] as String).find(/(\d+)/)), BufferUnit.valueOf((it["Resource_List"]["mem"] as String)[-2])) }
            if (it["Resource_List"]["nodect"])
                nodes = catchExceptionAndLog { Integer.valueOf(it["Resource_List"]["nodect"] as String) }
            if (it["Resource_List"]["nodes"])
                cores = catchExceptionAndLog { Integer.valueOf((it["Resource_List"]["nodes"] as String).find("ppn=.*").find(/(\d+)/)) }
            if (it["Resource_List"]["nodes"])
                additionalNodeFlag = catchExceptionAndLog { (it["Resource_List"]["nodes"] as String).find(/(\d+):(\.*)/) { fullMatch, nCores, feature -> return feature } }
            if (it["Resource_List"]["walltime"])
                walltime = catchExceptionAndLog { new TimeUnit(it["Resource_List"]["walltime"] as String) }

            BufferValue usedMem = null
            TimeUnit usedWalltime = null
            if (it["resources_used"]["mem"])
                catchExceptionAndLog { usedMem = new BufferValue(Integer.valueOf((it["resources_used"]["mem"] as String).find(/(\d+)/)), BufferUnit.valueOf((it["resources_used"]["mem"] as String)[-2]))  }
            if (it["resources_used"]["walltime"])
                catchExceptionAndLog { usedWalltime = new TimeUnit(it["resources_used"]["walltime"] as String) }

            gj.setAskedResources(new ResourceSet(null, mem, cores, nodes, walltime, null, it["queue"] as String, additionalNodeFlag))
            gj.setUsedResources(new ResourceSet(null, usedMem, null, null, usedWalltime, null, it["queue"] as String, null))

            gj.setLogFile(getQstatFile(it["Output_Path"] as String))
            gj.setErrorLogFile(getQstatFile(it["Error_Path"] as String))
            gj.setUser(it["euser"] as String)
            gj.setExecutionHosts(it["exec_host"] as String)
            gj.setSubmissionHost(it["submit_host"] as String)
            gj.setPriority(it["Priority"] as String)
            gj.setUserGroup(it["egroup"] as String)
            gj.setResourceReq(it["submit_args"] as String)
            gj.setRunTime(it["total_runtime"] ? catchExceptionAndLog { Duration.ofSeconds(Math.round(Double.parseDouble(it["total_runtime"] as String)), 0) } : null)
            gj.setCpuTime(it["resources_used"]["cput"] ? catchExceptionAndLog { parseColonSeparatedHHMMSSDuration(it["resources_used"]["cput"] as String) } : null)
            gj.setServer(it["server"] as String)
            gj.setUmask(it["umask"] as String)
            gj.setJobState(parseJobState(it["job_state"] as String))
            gj.setExitCode(it["exit_status"] ? catchExceptionAndLog { Integer.valueOf(it["exit_status"] as String) }: null )
            gj.setAccount(it["Account_Name"] as String)
            gj.setStartCount(it["start_count"] ? catchExceptionAndLog { Integer.valueOf(it["start_count"] as String) } : null)

            if (it["qtime"]) // The time that the job entered the current queue.
                gj.setSubmitTime(parseTime(it["qtime"] as String))
            if (it["start_time"]) // The timepoint the job was started.
                gj.setStartTime(parseTime(it["start_time"] as String))
            if (it["comp_time"])  // The timepoint the job was completed.
                gj.setEndTime(parseTime(it["comp_time"] as String))
            if (it["etime"])  // The time that the job became eligible to run, i.e. in a queued state while residing in an execution queue.
                gj.setEligibleTime(parseTime(it["etime"] as String))

            queriedExtendedStates.put(new BEJobID(it["Job_Id"] as String), gj)
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
        parameters.put('-q', queue)
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
