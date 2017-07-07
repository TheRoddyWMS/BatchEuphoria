/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */
package de.dkfz.roddy.execution.cluster.pbs

import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.TimeUnit

import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.regex.Matcher

/**
 * Process output of pbs qstat command
 *
 * Created by kaercher on 23.06.17.
 */
class PBSQstatReader {


    static public final String WITH_DELIMITER = '(?=(%1$s))'
    /**
     * Reads qstat output
     * @param qstatOutput
     * @return output of qstat in a map with jobid as key
     */
    static Map<String, Map<String, String>> readQstatOutput(String qstatOutput) {
        return qstatOutput.split(String.format(WITH_DELIMITER, "\n\nJob Id: ")).collectEntries {
            Matcher matcher = it =~ /^\s*Job Id: (?<jobId>\d+)\..*\n/
            if (matcher) {[
                    (matcher.group("jobId")):
                            it
            ]} else {
                [:]
            }
        }.collectEntries { String jobId, String value ->
            // join multi-line values
            value = value.replaceAll("\n\t", "")
            [(jobId):value]
        }.collectEntries { String jobId, String value ->
            Map<String, String> p = value.readLines().collectEntries { String line ->
                if (line.startsWith("    ") && line.contains(" = ")) {
                    String[] parts = line.split(" = ")
                    [parts.head().replaceAll(/^ {4}/, ""), parts.tail().join(' ')]
                } else {
                    [:]
                }
            }
            [(jobId): p]
        }
    }
    /**
     * Reads the qstat output and creates GenericJobInfo objects
     * @param resultLines - Input of ExecutionResult object
     * @return map with jobid as key
     */
    static Map<String, GenericJobInfo> processQstatOutput(List<String> resultLines) {
        Map<String, GenericJobInfo> queriedExtendedStates = [:]

        Map<String, Map<String, String>> qstatReaderResult = PBSQstatReader.readQstatOutput(resultLines.join("\n"))
        qstatReaderResult.each { it ->

            Map<String, String> jobResult = it.getValue()
            GenericJobInfo gj = new GenericJobInfo(jobResult.get("Job_Name"), null, it.getKey(), null, jobResult.get("depend") ? jobResult.get("depend").find("afterok.*")?.findAll(/(\d+).(\w+)/) { fullMatch, beforeDot, afterDot -> return beforeDot } : null)

            BufferValue mem = null
            int cores
            int nodes
            TimeUnit walltime = null
            String additionalNodeFlag

            if (jobResult.get("Resource_List.mem"))
                mem = new BufferValue(Integer.valueOf(jobResult.get("Resource_List.mem").find(/(\d+)/)), BufferUnit.valueOf(jobResult.get("Resource_List.mem")[-2]))
            if (jobResult.get("Resource_List.nodect"))
                nodes = Integer.valueOf(jobResult.get("Resource_List.nodect"))
            if (jobResult.get("Resource_List.nodes"))
                cores = Integer.valueOf(jobResult.get("Resource_List.nodes").find("ppn=.*").find(/(\d+)/))
            if (jobResult.get("Resource_List.nodes"))
                additionalNodeFlag = jobResult.get("Resource_List.nodes").find(/(\d+):(\.*)/) { fullMatch, nCores, feature -> return feature }
            if (jobResult.get("Resource_List.walltime"))
                walltime = new TimeUnit(jobResult.get("Resource_List.walltime"))

            BufferValue usedMem = null
            TimeUnit usedWalltime = null
            if (jobResult.get("resources_used.mem"))
                usedMem = new BufferValue(Integer.valueOf(jobResult.get("resources_used.mem").find(/(\d+)/)), BufferUnit.valueOf(jobResult.get("resources_used.mem")[-2]))
            if (jobResult.get("resources_used.walltime"))
                usedWalltime = new TimeUnit(jobResult.get("resources_used.walltime"))

            gj.setAskedResources(new ResourceSet(null, mem, cores, nodes, walltime, null, jobResult.get("queue"), additionalNodeFlag))
            gj.setUsedResources(new ResourceSet(null, usedMem, null, null, usedWalltime, null, jobResult.get("queue"), null))

            gj.setOutFile(jobResult.get("Output_Path"))
            gj.setErrorFile(jobResult.get("Error_Path"))
            gj.setUser(jobResult.get("euser"))
            gj.setExHosts(jobResult.get("exec_host"))
            gj.setSubHost(jobResult.get("submit_host"))
            gj.setPriority(jobResult.get("Priority"))
            gj.setUserGroup(jobResult.get("egroup"))
            gj.setResReq(jobResult.get("submit_args"))
            gj.setRunTime(jobResult.get("total_runtime") ? Duration.ofSeconds(Math.round(Double.parseDouble(jobResult.get("total_runtime"))), 0) : null)
            gj.setCpuTime(jobResult.get("resources_used.cput") ? Duration.parse("PT" + jobResult.get("resources_used.cput").substring(0, 2) + "H" + jobResult.get("resources_used.cput").substring(3, 5) + "M" + jobResult.get("resources_used.cput").substring(6) + "S") : null)
            gj.setServer(jobResult.get("server"))
            gj.setUmask(jobResult.get("umask"))
            gj.setJobState(JobState.parseJobState(jobResult.get("job_state")))
            gj.setExitCode(jobResult.get("exit_status"))

            DateTimeFormatter pbsDatePattern = DateTimeFormatter.ofPattern("EEE MMM ppd HH:mm:ss yyyy").withLocale(Locale.ENGLISH)
            if (jobResult.get("qtime"))
                gj.setSubTime(LocalDateTime.parse(jobResult.get("qtime"), pbsDatePattern))
            if (jobResult.get("start_time"))
                gj.setStartTime(LocalDateTime.parse(jobResult.get("start_time"), pbsDatePattern))
            if (jobResult.get("comp_time"))
                gj.setEndTime(LocalDateTime.parse(jobResult.get("comp_time"), pbsDatePattern))

            queriedExtendedStates.put(it.getKey(), gj)
        }
        return queriedExtendedStates
    }

}

