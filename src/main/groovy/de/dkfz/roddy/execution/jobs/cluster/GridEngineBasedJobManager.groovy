package de.dkfz.roddy.execution.jobs.cluster

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.LoggerWrapper
import de.dkfz.roddy.tools.RoddyConversionHelperMethods
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic
import groovy.util.slurpersupport.GPathResult
import org.xml.sax.SAXParseException

import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.regex.Matcher

@CompileStatic
abstract class GridEngineBasedJobManager<C extends Command> extends ClusterJobManager<C> {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(GridEngineBasedJobManager)

    public static final String WITH_DELIMITER = '(?=(%1$s))'

    GridEngineBasedJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    @Override
    boolean getDefaultForHoldJobsEnabled() { return true }

    @Override
    String getSubmissionCommand() {
        return "qsub"
    }

    protected int getPositionOfJobID() {
        return 0
    }

    protected int getPositionOfJobState() {
        return 4
    }

    @Override
    protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs) {
        StringBuilder queryCommand = new StringBuilder(getQueryJobStatesCommand())

        if (jobIDs && jobIDs.size() < 10) {
            queryCommand << " " << jobIDs*.id.join(" ")
        }

        if (isTrackingOfUserJobsEnabled)
            queryCommand << " -u $userIDForQueries "

        ExecutionResult er = executionService.execute(queryCommand.toString())
        List<String> resultLines = er.resultLines

        Map<BEJobID, JobState> result = [:]

        if (!er.successful) {
            if (strictMode) // Do not pull this into the outer if! The else branch needs to be executed if er.successful is true
                throw new BEException("The execution of ${queryCommand} failed.\n\t" + er.resultLines?.join("\n\t").toString())
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
                    if (logger.isVerbosityMedium())
                        logger.postAlwaysInfo(["QStat BEJob line: " + line,
                                               "	Entry in arr[" + ID + "]: " + split[ID],
                                               "    Entry in arr[" + JOBSTATE + "]: " + split[JOBSTATE]].join("\n"))

                    BEJobID jobID = new BEJobID(split[ID])

                    JobState js = parseJobState(split[JOBSTATE])
                    result.put(jobID, js)
                }
            }
        }
        return result
    }

    @Override
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates
        String qStatCommand = getExtendedQueryJobStatesCommand()
        qStatCommand += " " + jobIds.collect { it }.join(" ")

        if (isTrackingOfUserJobsEnabled)
            qStatCommand += " -u $userIDForQueries "

        ExecutionResult er
        try {
            er = executionService.execute(qStatCommand.toString())
        } catch (Exception exp) {
            logger.severe("Could not execute qStat command", exp)
        }

        if (er != null && er.successful) {
            queriedExtendedStates = this.processQstatOutputFromXML([er.resultLines.join("")])
        } else {
            throw new BEException("Extended job states couldn't be retrieved. \n Returned status code:${er.exitCode} \n ${qStatCommand.toString()} \n\t result:${er.resultLines.join("\n\t")}")
        }
        return queriedExtendedStates
    }

    @Override
    protected ExecutionResult executeStartHeldJobs(List<BEJobID> jobIDs) {
        String command = "qrls ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    @Override
    ExecutionResult executeKillJobs(List<BEJobID> jobIDs) {
        String command = "qdel ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }
    /**
     * Reads qstat output
     * @param qstatOutput
     * @return output of qstat in a map with jobid as key
     */
    static Map<String, Map<String, String>> processQstatOutputFromPlainText(String qstatOutput) {
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
        return catchAndLogExceptions { Instant.ofEpochSecond(Long.valueOf(str)).atZone(ZoneId.systemDefault()).toLocalDateTime() }
    }

    /**
     * Reads the qstat output and creates GenericJobInfo objects
     * @param resultLines - Input of ExecutionResult object
     * @return map with jobid as key
     */
    Map<BEJobID, GenericJobInfo> processQstatOutputFromXML(List<String> resultLines) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates = [:]
        if (resultLines.isEmpty()) {
            return [:]
        }

        GPathResult parsedJobs
        try {
            parsedJobs = new XmlSlurper().parseText(resultLines.last())
        } catch (SAXParseException ex) {
            logger.rare(resultLines.last())
            throw ex
        }

        for (it in parsedJobs.children()) {
            GenericJobInfo gj = new GenericJobInfo(it["Job_Name"] as String, null, it["Job_Id"] as String, null, it["depend"] ? (it["depend"] as String).find("afterok.*")?.findAll(/(\d+).(\w+)/) { fullMatch, beforeDot, afterDot -> return beforeDot } : null)

            BufferValue mem = null
            Integer cores
            Integer nodes
            TimeUnit walltime = null
            String additionalNodeFlag

            if (it["Resource_List"]["mem"])
                mem = catchAndLogExceptions { new BufferValue(Integer.valueOf((it["Resource_List"]["mem"] as String).find(/(\d+)/)), BufferUnit.valueOf((it["Resource_List"]["mem"] as String)[-2])) }
            if (it["Resource_List"]["nodect"])
                nodes = catchAndLogExceptions { Integer.valueOf(it["Resource_List"]["nodect"] as String) }
            if (it["Resource_List"]["nodes"])
                cores = catchAndLogExceptions { Integer.valueOf((it["Resource_List"]["nodes"] as String).find("ppn=.*").find(/(\d+)/)) }
            if (it["Resource_List"]["nodes"])
                additionalNodeFlag = catchAndLogExceptions { (it["Resource_List"]["nodes"] as String).find(/(\d+):(\.*)/) { fullMatch, nCores, feature -> return feature } }
            if (it["Resource_List"]["walltime"])
                walltime = catchAndLogExceptions { new TimeUnit(it["Resource_List"]["walltime"] as String) }

            BufferValue usedMem = null
            TimeUnit usedWalltime = null
            if (it["resources_used"]["mem"])
                catchAndLogExceptions { usedMem = new BufferValue(Integer.valueOf((it["resources_used"]["mem"] as String).find(/(\d+)/)), BufferUnit.valueOf((it["resources_used"]["mem"] as String)[-2])) }
            if (it["resources_used"]["walltime"])
                catchAndLogExceptions { usedWalltime = new TimeUnit(it["resources_used"]["walltime"] as String) }

            gj.setAskedResources(new ResourceSet(null, mem, cores, nodes, walltime, null, it["queue"] as String, additionalNodeFlag))
            gj.setUsedResources(new ResourceSet(null, usedMem, null, null, usedWalltime, null, it["queue"] as String, null))

            gj.setLogFile(getQstatFile(it["Output_Path"] as String))
            gj.setErrorLogFile(getQstatFile(it["Error_Path"] as String))
            gj.setUser(it["euser"] as String)
            gj.setExecutionHosts([it["exec_host"] as String])
            gj.setSubmissionHost(it["submit_host"] as String)
            gj.setPriority(it["Priority"] as String)
            gj.setUserGroup(it["egroup"] as String)
            gj.setResourceReq(it["submit_args"] as String)
            gj.setRunTime(it["total_runtime"] ? catchAndLogExceptions { Duration.ofSeconds(Math.round(Double.parseDouble(it["total_runtime"] as String)), 0) } : null)
            gj.setCpuTime(it["resources_used"]["cput"] ? catchAndLogExceptions { parseColonSeparatedHHMMSSDuration(it["resources_used"]["cput"] as String) } : null)
            gj.setServer(it["server"] as String)
            gj.setUmask(it["umask"] as String)
            gj.setJobState(parseJobState(it["job_state"] as String))
            gj.setExitCode(it["exit_status"] ? catchAndLogExceptions { Integer.valueOf(it["exit_status"] as String) } : null)
            gj.setAccount(it["Account_Name"] as String)
            gj.setStartCount(it["start_count"] ? catchAndLogExceptions { Integer.valueOf(it["start_count"] as String) } : null)

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

    @Override
    void createDefaultManagerParameters(LinkedHashMultimap<String, String> parameters) {

    }
}
