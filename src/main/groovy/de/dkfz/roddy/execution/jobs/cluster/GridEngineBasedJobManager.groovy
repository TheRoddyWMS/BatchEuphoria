/*
 * Copyright (c) 2019 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/BatchEuphoria/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.RoddyConversionHelperMethods
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic

import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

@CompileStatic
abstract class GridEngineBasedJobManager<C extends Command> extends ClusterJobManager<C> {

    public static final String WITH_DELIMITER = '(?=(%1$s))'
    private final ZoneId TIME_ZONE_ID

    @Override
    ZonedDateTime parseTime(String str) {
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(str as long), TIME_ZONE_ID)
    }

    GridEngineBasedJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
        TIME_ZONE_ID = parms.timeZoneId
    }

    @Override
    boolean getDefaultForHoldJobsEnabled() { return true }

    @Override
    String getSubmissionCommand() {
        return "qsub"
    }

    protected int getColumnOfJobID() {
        return 0
    }

    protected int getColumnOfJobState() {
        return 4
    }

    @Override
    Map<BEJobID, JobInfo> queryJobInfo(List<BEJobID> jobIDs) {
        String queryCommand = assembleJobInfoQueryCommand(jobIDs)

        ExecutionResult er = executionService.execute(queryCommand)
        List<String> resultLines = er.resultLines

        if (!er.successful)
            throw new BEException("The execution of ${queryCommand} failed.\n\t" + er.resultLines?.join("\n\t")?.toString())

        if (resultLines.size() < 2)
            return [:]

        Map<BEJobID, JobInfo> result = [:]
        for (String line : resultLines) {
            line = line.trim()
            if (line.length() == 0) continue
            if (!RoddyConversionHelperMethods.isInteger(line.substring(0, 1)))
                continue //Filter out lines which have been missed which do not start with a number.

            String[] split = line.split("\\s+")
            final int ID = getColumnOfJobID()
            final int JOBSTATE = getColumnOfJobState()

            BEJobID jobID = new BEJobID(split[ID])
            if (jobIDs && !jobIDs.contains(jobID))
                continue //Ignore ids which are not queried but keep them, if we don't use a filter (jobIDs).

            JobState jobState = parseJobState(split[JOBSTATE])
            def info = new JobInfo(jobID)
            info.jobState = jobState

            result[jobID] = info
        }
        return result
    }

    String assembleJobInfoQueryCommand(List<BEJobID> jobIDs) {
        StringBuilder queryCommand = new StringBuilder(getQueryCommandForJobInfo())

        if (jobIDs && jobIDs.size() < 10)
            queryCommand << " " << jobIDs*.id.join(" ")

        if (isTrackingOfUserJobsEnabled)
            queryCommand << " -u $userIDForQueries "
        queryCommand.toString()
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

    BufferValue safelyCastToBufferValue(Object value) {
        String _value = value as String
        if (_value)
            return withCaughtAndLoggedException { new BufferValue(Integer.valueOf(_value.find(/(\d+)/)), BufferUnit.valueOf(_value[-2])) }
        return null
    }

    Integer safelyCastToInteger(Object value) {
        if (value)
            return withCaughtAndLoggedException { return Integer.valueOf(value as String) }
        return null
    }

    Duration safelyCastToDuration(Object value) {
        String _value = value as String
        if (_value)
            return withCaughtAndLoggedException { Duration.ofSeconds(Math.round(Double.parseDouble(_value)), 0) }
        return null
    }


    TimeUnit safelyCastToTimeUnit(Object value) {
        String _value = value as String
        if (_value)
            return withCaughtAndLoggedException { new TimeUnit(_value) }
        return null
    }

    static List<String> getExecutionHosts(Object hosts) {
        String _hosts = hosts as String
        if (!_hosts) {
            return []
        }
        withCaughtAndLoggedException {
            _hosts.split(/\+/)
                    .collect { String str -> str.split("/") }
                    .collect { it.first() }
                    .unique()
        }
    }

    static List<String> getJobDependencies(Object deps) {
        String _deps = deps as String
        if (!_deps) {
            return []
        }
        withCaughtAndLoggedException {
            return _deps.split(",")
                    .find { it.startsWith("afterok") }
                    ?.findAll(/(\d+)(\.\w+)?/) { fullMatch, String beforeDot, afterDot -> return beforeDot } ?: [] as List<String>
        }
    }

    File safelyGetQstatFile(Object s, String jobId) {
        String _s = s as String
        if (!_s) {
            return null
        }
        withCaughtAndLoggedException {
            String fileName
            if (_s.startsWith("/")) {
                fileName = _s
            } else if (_s =~ /^[\w-]+:\//) {
                fileName = _s.replaceAll(/^[\w-]+:/, "")
            } else {
                return null
            }
            new File(fileName.replace("\$${getJobIdVariable()}", jobId))
        }
    }

    @Override
    void createDefaultManagerParameters(LinkedHashMultimap<String, String> parameters) {

    }
}
