/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.tools.AnyEscapableString
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.RoddyConversionHelperMethods
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic
import groovy.util.slurpersupport.GPathResult

import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.regex.Matcher

@CompileStatic
abstract class GridEngineBasedJobManager<C extends Command> extends ClusterJobManager<C> {

    public static final String WITH_DELIMITER = '(?=(%1$s))'
    private final ZoneId TIME_ZONE_ID


    GridEngineBasedJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
        TIME_ZONE_ID = parms.timeZoneId
    }

    @Override
    boolean getDefaultForHoldJobsEnabled() { return true }

    protected int getColumnOfJobID() {
        return 0
    }

    protected int getColumnOfJobState() {
        return 4
    }

    @Override
    protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs,
                                                    Duration timeout = Duration.ZERO) {
        StringBuilder queryCommand = new StringBuilder(getQueryJobStatesCommand())

        if (jobIDs && jobIDs.size() < 10) {
            queryCommand << " " << jobIDs*.id.join(" ")
        }

        if (isTrackingOfUserJobsEnabled)
            queryCommand << " -u $userIDForQueries "

        ExecutionResult er = executionService.execute(queryCommand.toString(), timeout)
        List<String> resultLines = er.stdout

        Map<BEJobID, JobState> result = [:]

        if (!er.successful) {
            throw new BEException("Execution failed. ${er.toStatusLine()}")
        } else {
            if (resultLines.size() > 2) {

                for (String line : resultLines) {
                    line = line.trim()
                    if (line.length() == 0) continue
                    if (!RoddyConversionHelperMethods.isInteger(line.substring(0, 1)))
                        continue //Filter out lines which have been missed which do not start with a number.

                    String[] split = line.split("\\s+")
                    final int ID = getColumnOfJobID()
                    final int JOBSTATE = getColumnOfJobState()

                    BEJobID jobID = new BEJobID(split[ID])

                    JobState js = parseJobState(split[JOBSTATE])
                    result.put(jobID, js)
                }
            }
        }
        return result
    }

    @Override
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds,
                                                           Duration timeout = Duration.ZERO) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates
        String qStatCommand = getExtendedQueryJobStatesCommand()
        qStatCommand += " " + jobIds.collect { it }.join(" ")

        if (isTrackingOfUserJobsEnabled)
            qStatCommand += " -u $userIDForQueries "

        ExecutionResult er = executionService.execute(qStatCommand.toString(), timeout)

        if (er != null && er.successful) {
            queriedExtendedStates = this.processQstatOutputFromXML(er.stdout.join("\n"))
        } else {
            throw new BEException("Extended job states couldn't be retrieved: ${er.toStatusLine()}")
        }
        return queriedExtendedStates
    }

    @Override
    protected ExecutionResult executeStartHeldJobs(List<BEJobID> jobIDs) {
        String command = "qrls ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false, commandTimeout)
    }

    @Override
    ExecutionResult executeKillJobs(List<BEJobID> jobIDs) {
        String command = "qdel ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false, commandTimeout)
    }

    /**
     * Reads qstat output
     * @param qstatOutput
     * @return output of qstat in a map with jobid as key
     */
    private static Map<String, Map<String, String>> processQstatOutputFromPlainText(String qstatOutput) {
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

    private ZonedDateTime parseTime(String str) {
        return withCaughtAndLoggedException { ZonedDateTime.ofInstant(Instant.ofEpochSecond(str as long), TIME_ZONE_ID) }
    }

    /**
     * Reads the qstat output and creates GenericJobInfo objects
     * @param resultLines - Input of ExecutionResult object
     * @return map with jobid as key
     */
    protected Map<BEJobID, GenericJobInfo> processQstatOutputFromXML(String result) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates = [:]
        if (result.isEmpty()) {
            return [:]
        }

        GPathResult parsedJobs = new XmlSlurper().parseText(result)

        for (job in parsedJobs.children()) {
            String jobIdRaw = job["Job_Id"] as String
            BEJobID jobID
            try {
                jobID = new BEJobID(jobIdRaw)
            } catch (Exception exp) {
                throw new BEException("Job ID '${jobIdRaw}' could not be transformed to BEJobID ")
            }

            List<String> jobDependencies = withCaughtAndLoggedException { getJobDependencies(job["depend"] as String) }
            String jobName = job["Job_Name"] as String ?: null
            GenericJobInfo gj = new GenericJobInfo(jobName, null, jobID, null, jobDependencies)

            BufferValue mem = null
            Integer cores
            Integer nodes
            TimeUnit walltime = null
            String additionalNodeFlag

            Object resourceList = job["Resource_List"]
            String resourcesListMem = resourceList["mem"] as String
            String resourcesListNoDect = resourceList["nodect"] as String
            String resourcesListNodes = resourceList["nodes"] as String
            String resourcesListWalltime = resourceList["walltime"] as String
            if (resourcesListMem)
                mem = withCaughtAndLoggedException { new BufferValue(Integer.valueOf(resourcesListMem.find(/(\d+)/)), BufferUnit.valueOf(resourcesListMem[-2])) }
            if (resourcesListNoDect)
                nodes = withCaughtAndLoggedException { Integer.valueOf(resourcesListNoDect) }
            if (resourcesListNodes)
                cores = withCaughtAndLoggedException { Integer.valueOf(resourcesListNodes.find("ppn=.*").find(/(\d+)/)) }
            if (resourcesListNodes)
                additionalNodeFlag = withCaughtAndLoggedException { resourcesListNodes.find(/(\d+):(\.*)/) { fullMatch, nCores, feature -> return feature } }
            if (resourcesListWalltime)
                walltime = withCaughtAndLoggedException { new TimeUnit(resourcesListWalltime) }

            BufferValue usedMem = null
            TimeUnit usedWalltime = null
            Object resourcesUsed = job["resources_used"]
            String resourcedUsedMem = resourcesUsed["mem"] as String
            String resourcesUsedWalltime = resourcesUsed["walltime"] as String
            if (resourcedUsedMem)
                withCaughtAndLoggedException { usedMem = new BufferValue(Integer.valueOf(resourcedUsedMem.find(/(\d+)/)), BufferUnit.valueOf(resourcedUsedMem[-2])) }
            if (resourcesUsedWalltime)
                withCaughtAndLoggedException { usedWalltime = new TimeUnit(resourcesUsedWalltime) }

            gj.setAskedResources(new ResourceSet(null, mem, cores, nodes, walltime, null, job["queue"] as String ?: null, additionalNodeFlag))
            gj.setUsedResources(new ResourceSet(null, usedMem, null, null, usedWalltime, null, job["queue"] as String ?: null, null))

            gj.setLogFile(withCaughtAndLoggedException { getQstatFile(job["Output_Path"] as String, jobIdRaw) })
            gj.setErrorLogFile(withCaughtAndLoggedException { getQstatFile(job["Error_Path"] as String, jobIdRaw) })
            gj.setUser(job["euser"] as String ?: null)
            gj.setExecutionHosts(withCaughtAndLoggedException { getExecutionHosts(job["exec_host"] as String) })
            gj.setSubmissionHost(job["submit_host"] as String ?: null)
            gj.setPriority(job["Priority"] as String ?: null)
            gj.setUserGroup(job["egroup"] as String ?: null)
            gj.setResourceReq(job["submit_args"] as String ?: null)
            gj.setRunTime(job["total_runtime"] ? withCaughtAndLoggedException { Duration.ofSeconds(Math.round(Double.parseDouble(job["total_runtime"] as String)), 0) } : null)
            gj.setCpuTime(resourcesUsed["cput"] ? withCaughtAndLoggedException { parseColonSeparatedHHMMSSDuration(job["resources_used"]["cput"] as String) } : null)
            gj.setServer(job["server"] as String ?: null)
            gj.setUmask(job["umask"] as String ?: null)
            gj.setJobState(parseJobState(job["job_state"] as String))
            gj.setExitCode(job["exit_status"] ? withCaughtAndLoggedException { Integer.valueOf(job["exit_status"] as String) }: null )
            gj.setAccount(job["Account_Name"] as String ?: null)
            gj.setStartCount(job["start_count"] ? withCaughtAndLoggedException { Integer.valueOf(job["start_count"] as String) } : null)

            if (job["qtime"]) // The time that the job entered the current queue.
                gj.setSubmitTime(parseTime(job["qtime"] as String))
            if (job["start_time"]) // The timepoint the job was started.
                gj.setStartTime(parseTime(job["start_time"] as String))
            if (job["comp_time"])  // The timepoint the job was completed.
                gj.setEndTime(parseTime(job["comp_time"] as String))
            if (job["etime"])  // The time that the job became eligible to run, i.e. in a queued state while residing in an execution queue.
                gj.setEligibleTime(parseTime(job["etime"] as String))

            queriedExtendedStates.put(jobID, gj)
        }
        return queriedExtendedStates
    }

    private static List<String> getExecutionHosts(String s) {
        if (!s) {
            return null
        }
        s.split(/\+/)
                .collect { String it -> it.split("/") }
                .collect { it.first() }
                .unique()
    }

    private static List<String> getJobDependencies(String s) {
        if (!s) {
            return []
        }
        s.split(",")
                .find { it.startsWith("afterok") }
                ?.findAll(/(\d+)(\.\w+)?/) { fullMatch, String beforeDot, afterDot -> return beforeDot } ?: []as List<String>
    }

    private File getQstatFile(String s, String jobId) {
        if (!s) {
            return null
        }
        String fileName
        if (s.startsWith("/")) {
            fileName = s
        } else if (s =~ /^[\w-]+:\//) {
            fileName = s.replaceAll(/^[\w-]+:/, "")
        } else {
            return null
        }
        new File(fileName.replace("\$$jobIdVariable", jobId))
    }

    @Override
    void createDefaultManagerParameters(LinkedHashMultimap<String, AnyEscapableString> parameters) {

    }
}
