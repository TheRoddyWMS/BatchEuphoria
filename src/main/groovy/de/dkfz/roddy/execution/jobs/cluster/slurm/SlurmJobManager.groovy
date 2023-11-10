/*
 * Copyright (c) 2022 German Cancer Research Center (DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.slurm

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.GridEngineBasedJobManager
import de.dkfz.roddy.tools.*
import groovy.json.JsonSlurper
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic

import java.time.*
import java.time.format.DateTimeFormatter

@CompileStatic
class SlurmJobManager extends GridEngineBasedJobManager {

    private final ZoneId TIME_ZONE_ID

    private final Map<String, JobState> stateMap = [
            "RUNNING"   : JobState.RUNNING,
            "SUSPENDED" : JobState.SUSPENDED,
            "PENDING"   : JobState.HOLD,
            "CANCELLED" : JobState.ABORTED,
            "CANCELLED+": JobState.ABORTED,
            "COMPLETED" : JobState.COMPLETED_SUCCESSFUL,
            "NODE_FAIL" : JobState.FAILED,
            "FAILED"    : JobState.FAILED,
    ]

    SlurmJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
        TIME_ZONE_ID = parms.timeZoneId
    }

    @Override
    protected SlurmSubmissionCommand createCommand(BEJob job) {
        SlurmSubmissionCommand ssc = new SlurmSubmissionCommand(
                this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id)
        return ssc
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String commandString) {
        throw new BEException("parseGenericJobInfo is not implemented")
    }


    @Override
    String parseJobID(String commandOutput) {
        return commandOutput
    }

    @Override
    protected int getColumnOfJobID() {
        return 0
    }

    @Override
    protected int getColumnOfJobState() {
        return 1
    }

    @Override
    protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs,
                                                    Duration timeout = Duration.ZERO) {
        StringBuilder queryCommand = new StringBuilder(getQueryJobStatesCommand())
        if (jobIDs && !jobIDs.empty) {
            queryCommand << " -j " << jobIDs.collect { it }.join(",")
        }

        if (isTrackingOfUserJobsEnabled)
            queryCommand << " -u $userIDForQueries "

        ExecutionResult er = executionService.execute(queryCommand.toString())
        List<String> resultLines = er.stdout

        Map<BEJobID, JobState> result = [:]

        if (!er.successful) {
            throw new BEException("Execution failed. ${er.toStatusLine()}")
        } else {
            if (resultLines.size() > 0) {
                for (String line : resultLines) {
                    line = line.trim()
                    if (line.length() == 0) continue
                    if (!RoddyConversionHelperMethods.isInteger(line.substring(0, 1)))
                        continue //Filter out lines which have been missed which do not start with a number.

                    String[] split = line.split("\\|")
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
    protected JobState parseJobState(String stateString) {
        return stateMap.get(stateString, JobState.UNKNOWN)
    }

    /**
     * For SLURM there are two different commands to query extended job states.
     * One only available during runtime that provides specific runtime info.
     * And one that can be run at any time, but does not provide the specific runtime info provided by the other command.
     * Since this is not supported by the superclass this implementation was chosen.
     */
    @Override
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds,
                                                           Duration timeout = Duration.ZERO) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates = [:]
        for (int i = 0; i < jobIds.size(); i++) {
            ExecutionResult er = executionService.execute("${getExtendedQueryJobStatesCommand()} ${jobIds[i]} -o")
            GenericJobInfo genericJobInfo
            if (er?.successful) {
                genericJobInfo = this.processSControlOutput(er.stdout.join("\n"))
                er = executionService.execute("sacct -j ${jobIds[i]} --json")
                if (er?.successful) {
                    GenericJobInfo primary = this.processSacctOutputFromJson(er.stdout.join("\n"))
                    if (primary) {
                        genericJobInfo = fillFromSupplement(primary, genericJobInfo)
                    }
                }
            } else {
                er = executionService.execute("sacct -j ${jobIds[i]} --json")
                if (er?.successful) {
                    genericJobInfo = this.processSacctOutputFromJson(er.stdout.join("\n"))
                } else {
                    throw new BEException("Extended job states couldn't be retrieved: ${er.toStatusLine()}")
                }
            }
            if (genericJobInfo) {
                queriedExtendedStates << [(jobIds[i]): genericJobInfo]
            } else {
                log.warn("Could not query extended states for ${jobIds[i]}")
            }
        }

        return queriedExtendedStates
    }

    @CompileDynamic
    protected static GenericJobInfo fillFromSupplement(GenericJobInfo primary, GenericJobInfo supplement) {
        if (!supplement) {
            return primary
        }
        primary.properties.findAll { k, v ->
            if (!v && supplement."${k}") {
                primary."${k}" = supplement."${k}"
            }
        }
        return primary
    }

    /**
     * Reads the scontrol output and creates GenericJobInfo objects
     * @param resultLines - Input of ExecutionResult object
     * @return GenericJobInfo
     */
    protected GenericJobInfo processSControlOutput(String stdout) {
        // Create a complex line object which will be used for further parsing.
        ComplexLine line = new ComplexLine(stdout)

        Collection<String> splitted = line.splitBy(" ").findAll { it }
        if (splitted.size() <= 1) {
            return null
        }
        Map<String, String> jobResult = parseExtendedOutput(splitted)

        BEJobID jobID
        String JOBID = jobResult["JobId"]
        try {
            jobID = new BEJobID(JOBID)
        } catch (Exception exp) {
            throw new BEException("Job ID '${JOBID}' could not be transformed to BEJobID ")
        }
        List<String> dependIDs = []
        GenericJobInfo jobInfo = new GenericJobInfo(jobResult["JobName"], new File(jobResult["Command"]), jobID, null, dependIDs)

        /** Directories and files */
        jobInfo.inputFile = new File(jobResult["StdIn"])
        jobInfo.logFile = new File(jobResult["StdOut"])
        jobInfo.user = jobResult["UserId"]
        jobInfo.submissionHost = jobResult["BatchHost"]
        jobInfo.executionHosts = [jobResult["NodeList"] as String]
        jobInfo.errorLogFile = new File(jobResult["StdErr"])
        jobInfo.execHome = jobResult["WorkDir"]

        /** Status info */
        jobInfo.jobState = parseJobState(jobResult["JobState"])
        jobInfo.exitCode = jobInfo.jobState == JobState.COMPLETED_SUCCESSFUL ? 0 : (jobResult["ExitCode"].split(":")[0] as Integer)
        jobInfo.pendReason = jobResult["Reason"]

        /** Resources */
        String queue = jobResult["Partition"]
        Duration runLimit = safelyParseColonSeparatedDuration(jobResult["TimeLimit"])
        Duration runTime = safelyParseColonSeparatedDuration(jobResult["RunTime"])
        jobInfo.runTime = runTime
        BufferValue memory = safelyCastToBufferValue(jobResult["mem"])
        Integer cores = withCaughtAndLoggedException { jobResult["cpu"] as Integer }
        Integer nodes = withCaughtAndLoggedException { jobResult["node"]?.split("-")?.last() as Integer }

        /**
         * The Info about usedResources is not available, but to be consistent with the output expected
         * of queryExtendedJobStateById they are both set here.
         **/
        jobInfo.askedResources = new ResourceSet(memory, cores, nodes, runLimit, null, queue, null)
        jobInfo.usedResources = new ResourceSet(memory, cores, nodes, runTime, null, queue, null)

        /** Timestamps */
        jobInfo.submitTime = parseTime(jobResult["SubmitTime"])
        jobInfo.eligibleTime = parseTime(jobResult["EligibleTime"])
        jobInfo.startTime = parseTime(jobResult["StartTime"])
        jobInfo.endTime = parseTime(jobResult["EndTime"])

        return jobInfo
    }

    private static Map<String, String> parseExtendedOutput(Collection<String> splitted) {
        Map<String, String> jobResult = [:]
        for (int i = 0; i < splitted.size(); i++) {
            String[] jobKeyValue = splitted[i].split("=|,")
            if (jobKeyValue.size() == 2) { //JobId=267858
                jobResult.put(jobKeyValue[0].trim(), jobKeyValue[1].trim())
            } else if (jobKeyValue.size() % 2 == 1) { //TRES=cpu=76,mem=300G,node=1,billing=151
                for (int j = 1; j < jobKeyValue.size(); j++) {
                    jobResult.put(jobKeyValue[j].trim(), jobKeyValue[++j].trim())
                }
            }
        }
        return jobResult
    }

    protected static Duration safelyParseColonSeparatedDuration(String value) {
        withCaughtAndLoggedException {
            if (value.contains("-")) {
                return Duration.ofDays(value.substring(0, value.lastIndexOf("-")).toLong()) +
                        parseColonSeparatedHHMMSSDuration(value.substring(value.lastIndexOf("-") + 1))
            } else {
                return parseColonSeparatedHHMMSSDuration(value)
            }
        }
    }

    /**
     * Reads the sacct output for a single job as Json and creates GenericJobInfo objects
     * @param resultLines - Input of ExecutionResult object
     * @return map with jobid as key
     */
    protected GenericJobInfo processSacctOutputFromJson(String rawJson) {
        if (!rawJson) {
            return null
        }

        Object jsonEntry
        Object parsedJson = new JsonSlurper().parseText(rawJson)
        List records = (List) parsedJson["jobs"]
        if (records.size() == 0) {
            return null
        } else if (records.size() != 1) {
            log.debug("There were ${records.size()} records.")
            for (entry in records) {
                if (entry["state"]["current"] == "REQUEUED") {
                    log.debug("Found requeued record.")
                } else {
                    if (jsonEntry) {
                        log.warn("Overwriting entry, state was: ${entry["state"]["current"]}")
                    }
                    jsonEntry = entry
                }
            }
            if (!jsonEntry) {
                throw new BEException("There is a problem with the sacct output. No valid entry found.")
            }
        } else {
            jsonEntry = records[0]
        }
        GenericJobInfo jobInfo
        BEJobID jobID
        String JOBID = jsonEntry["job_id"]
        try {
            jobID = new BEJobID(JOBID)
        } catch (Exception exp) {
            throw new BEException("Job ID '${JOBID}' could not be transformed to BEJobID ")
        }

        List<String> dependIDs = []
        jobInfo = new GenericJobInfo(jsonEntry["name"] as String, null, jobID, null, dependIDs)

        /** Common */
        jobInfo.user = jsonEntry["user"]
        jobInfo.userGroup = jsonEntry["group"]
        jobInfo.jobGroup = jsonEntry["group"]
        jobInfo.priority = jsonEntry["priority"]
        jobInfo.executionHosts = [jsonEntry["nodes"] as String]

        /** Status info */
        jobInfo.jobState = parseJobState(jsonEntry["state"]["current"] as String)
        jobInfo.exitCode = jobInfo.jobState == JobState.COMPLETED_SUCCESSFUL ? 0 : (jsonEntry["exit_code"]["return_code"] as Integer)
        jobInfo.pendReason = jsonEntry["state"]["reason"]

        /** Resources */
        String queue = jsonEntry["partition"]
        Duration runLimit = Duration.ofMinutes(jsonEntry["time"]["limit"] as long)
        Duration runTime = Duration.ofSeconds(jsonEntry["time"]["elapsed"] as long)
        BufferValue memory = safelyCastToBufferValue(jsonEntry["required"]["memory"] as String)
        Integer cores = withCaughtAndLoggedException { jsonEntry["required"]["CPUs"] as Integer }
        cores = cores == 0 ? null : cores
        Integer nodes = withCaughtAndLoggedException { jsonEntry["allocation_nodes"] as Integer }

        /**
         * The Info about usedResources is not available, but to be consistent with the output expected
         * of queryExtendedJobStateById they are both set here.
         **/
        jobInfo.askedResources = new ResourceSet(memory, cores, nodes, runLimit, null, queue, null)
        jobInfo.usedResources = new ResourceSet(memory, cores, nodes, runTime, null, queue, null)
        jobInfo.runTime = runTime

        /** Directories and files */
        jobInfo.execHome = jsonEntry["working_directory"]

        /** Timestamps */
        jobInfo.submitTime = parseTimeOfEpochSecond(jsonEntry["time"]["submission"] as String)
        String eligibleTime = jsonEntry["time"]["eligible"] as String
        jobInfo.eligibleTime = eligibleTime != "0" ? parseTimeOfEpochSecond(eligibleTime) : null // convert "0" to null
        jobInfo.endTime = parseTimeOfEpochSecond(jsonEntry["time"]["end"] as String)
        /**
         * When startTime is "0", endTime should be used. Otherwise parseTimeOfEpochSecond will return 1970.
         * Null here is not an option with how this value is currently used.
         **/
        String startTime = jsonEntry["time"]["start"] as String
        jobInfo.startTime = startTime != "0" ? parseTimeOfEpochSecond(startTime) : jobInfo.endTime


        return jobInfo
    }

    private ZonedDateTime parseTime(String str) {
        return str != 'Unknown' ? withCaughtAndLoggedException {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
            LocalDateTime localDateTime = LocalDateTime.parse(str, formatter)
            return ZonedDateTime.of(localDateTime, TIME_ZONE_ID)
        } : null
    }

    private ZonedDateTime parseTimeOfEpochSecond(String str) {
        return str != 'Unknown' ? withCaughtAndLoggedException { ZonedDateTime.ofInstant(Instant.ofEpochSecond(str as long), TIME_ZONE_ID) } : null
    }

    BufferValue safelyCastToBufferValue(String MAX_MEM) {
        withCaughtAndLoggedException {
            if (MAX_MEM) {
                String bufferSize = MAX_MEM.find("([0-9]*[.])?[0-9]+")
                String unit = MAX_MEM.find("[a-zA-Z]+")
                BufferUnit bufferUnit = unit == "G" ? BufferUnit.g : BufferUnit.m
                return new BufferValue(bufferSize, bufferUnit)
            }
            return null
        }
    }


    @Override
    protected ExecutionResult executeStartHeldJobs(List<BEJobID> jobIDs) {
        String command = "scontrol release ${jobIDs*.id.join(",")}"
        return executionService.execute(command, false)
    }

    @Override
    ExecutionResult executeKillJobs(List<BEJobID> jobIDs) {
        String command = "scancel ${jobIDs*.id.join(",")}"
        return executionService.execute(command, false)
    }

    @Override
    List<String> getEnvironmentVariableGlobs() {
        return Collections.unmodifiableList(["SLURM_*"])
    }

    @Override
    String getQueryJobStatesCommand() {
        return "squeue --format='%i|%T'"
    }

    @Override
    String getExtendedQueryJobStatesCommand() {
        return "scontrol show job"
    }

    @Override
    void createComputeParameter(ResourceSet resourceSet, LinkedHashMultimap<String, String> parameters) {
        int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
        int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1
        String nVal = "--nodes=" + nodes
        String cVal = " --cores-per-socket=" + cores
        parameters.put(nVal, cVal)
    }

    void createQueueParameter(LinkedHashMultimap<String, String> parameters, String queue) {
        parameters.put('-p', queue)
    }

    @Override
    void createWalltimeParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put('--time=' + TimeUnit.fromDuration(resourceSet.walltime).toHourString(), " ")
    }

    @Override
    void createMemoryParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put('--mem=' + resourceSet.getMem().toString(BufferUnit.M), " ")
    }

    @Override
    void createStorageParameters(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
    }

    @Override
    String getJobIdVariable() {
        return "SLURM_JOB_ID"
    }

    @Override
    String getJobNameVariable() {
        return "SLURM_JOB_NAME"
    }

    @Override
    String getNodeFileVariable() {
        return "SLURM_JOB_NODELIST"
    }

    @Override
    String getSubmitHostVariable() {
        return "SLURM_SUBMIT_HOST"
    }

    @Override
    String getSubmitDirectoryVariable() {
        return "SLURM_SUBMIT_DIR"
    }

    @Override
    String getQueueVariable() {
        return "SLURM_JOB_PARTITION"
    }

    @Override
    String getSubmissionCommand() {
        return "sbatch"
    }

}
