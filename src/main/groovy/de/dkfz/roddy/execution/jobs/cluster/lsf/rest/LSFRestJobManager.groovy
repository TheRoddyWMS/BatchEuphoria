/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf.rest

import de.dkfz.roddy.BEException
import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.AnyEscapableString
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.BashInterpreter
import de.dkfz.roddy.execution.RestExecutionService
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.lsf.AbstractLSFJobManager
import de.dkfz.roddy.execution.jobs.cluster.lsf.LSFSubmissionCommand
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import groovy.transform.CompileStatic
import groovy.util.slurpersupport.GPathResult
import groovy.util.slurpersupport.NodeChild
import org.apache.commons.text.StringEscapeUtils
import org.apache.http.Header
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.protocol.HTTP

import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import static de.dkfz.roddy.execution.EscapableString.*

/**
 * REST job manager for cluster systems.
 *
 */
@CompileStatic
class LSFRestJobManager extends AbstractLSFJobManager {

    protected final RestExecutionService restExecutionService

    /*REST RESOURCES*/
    private static String URI_JOB_SUBMIT = "/jobs/submit"
    private static String URI_JOB_KILL = "/jobs/kill"
    private static String URI_JOB_SUSPEND = "/jobs/suspend"
    private static String URI_JOB_RESUME = "/jobs/resume"
    private static String URI_JOB_REQUEUE = "/jobs/requeue"
    private static String URI_JOB_DETAILS = "/jobs/"
    private static String URI_JOB_BASICS = "/jobs/basicinfo"
    private static String URI_JOB_HISTORY = "/jobhistory"
    private static String URI_USER_COMMAND = "/userCmd"

    private static Integer HTTP_OK = 200

    private static String NEW_LINE = "\r\n"

    private final DateTimeFormatter DATE_PATTERN


    LSFRestJobManager(BEExecutionService restExecutionService, JobManagerOptions parms) {
        super(restExecutionService, parms)
        this.restExecutionService = restExecutionService as RestExecutionService
        DATE_PATTERN = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")
            .withLocale(Locale.ENGLISH)
            .withZone(parms.timeZoneId)
    }

    /**
     * Submit job
     *
     body = "--bqJky99mlBWa-ZuqjC53mG6EzbmlxB\r\n" +
     "Content-Disposition: form-data; name=\"AppName\"\r\n" +
     "Content-ID: <AppName>\r\n" +
     "\r\n" +
     "generic\r\n" +
     "--bqJky99mlBWa-ZuqjC53mG6EzbmlxB\r\n" +
     "Content-Disposition: form-data; name=\"data\"\r\n" +
     "Content-Type: multipart/mixed; boundary=_Part_1_701508.1145579811786\r\n" +
     "Content-ID: <data>\r\n" +
     "\r\n" +
     "--_Part_1_701508.1145579811786\r\n" +
     "Content-Disposition: form-data; name=\"COMMANDTORUN\"\r\n" +
     "Content-Type: application/xml; charset=UTF-8\r\n" +
     "\r\n" +
     "<AppParam><id>COMMANDTORUN</id><value>sleep 100</value><type></type></AppParam>\r\n" +
     "--_Part_1_701508.1145579811786\r\n" +
     "Content-Disposition: form-data; name=\"EXTRA_PARAMS\"\r\n" +
     "Content-Type: application/xml; charset=UTF-8\r\n" +
     "\r\n" +
     "<AppParam><id>EXTRA_PARAMS</id><value>-R 'select[type==any]'</value><type></type></AppParam>\r\n" +
     "--_Part_1_701508.1145579811786\r\n" +
     "\r\n" +
     "--bqJky99mlBWa-ZuqjC53mG6EzbmlxB--\r\n"
     */
    @Override
    RestSubmissionCommand createCommand(BEJob job) {
        List<Header> headers = []
        headers << new BasicHeader("Accept", "text/xml,application/xml;")

        List<String> requestParts = []
        requestParts << createRequestPart("AppName", "generic")

        // --- Parameters Area ---
        List<String> jobParts = []
        List<AnyEscapableString> command = job.command
        if (command) {
            jobParts << createJobPart("COMMAND", BashInterpreter.instance.interpret(join(command, " ")), "COMMANDTORUN")
        } else {
            jobParts << createJobPart("COMMAND", "${job.jobName},upload" as String, "COMMANDTORUN", "file")
        }
        if (job.getJobName()) {
            jobParts << createJobPart("JOB_NAME", job.jobName)
        }
        jobParts << createJobPart("EXTRA_PARAMS", prepareExtraParams(job))
        ContentWithHeaders jobPartsWithHeader = joinParts(jobParts)
        requestParts << createRequestPart("data", jobPartsWithHeader.content, jobPartsWithHeader.headers)

        if (job.code) {
            requestParts << createRequestPart("f1", BashInterpreter.instance.interpret(job.code), [
                    new BasicHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_OCTET_STREAM.toString()),
            ] as List<Header>, job.jobName)
        }

        ContentWithHeaders requestPartsWithHeader = joinParts(requestParts)
        headers.addAll(requestPartsWithHeader.headers)

        return new RestSubmissionCommand(URI_JOB_SUBMIT, requestPartsWithHeader.content, headers, RestSubmissionCommand.HttpMethod.HTTPPOST)
    }

    @Override
    protected String parseJobID(String commandOutput) {
        return new XmlSlurper().parseText(commandOutput)
    }


    @Override
    GenericJobInfo parseGenericJobInfo(String command) {
        return null
    }

    /**
     * Create a part of a multipart request in the special format required
     */
    private static String createRequestPart(String name, String value, List<Header> additionalHeaders = [], String id = name) {
        List<Header> headers = additionalHeaders
        headers.add(0, new BasicHeader("Content-Disposition", "form-data; name=\"${name}\""))
        headers << new BasicHeader("Content-ID", "<${id}>")
        return "${headers.join(NEW_LINE)}${NEW_LINE}${NEW_LINE}${value}${NEW_LINE}"
    }

    /**
     * Create a part of a multipart document in the format required for parameters
     */
    private static String createJobPart(String name, String value, String id = name, String type = "") {
        return """\
        Content-Disposition: form-data; name="${name}"
        Content-Type: application/xml; charset=UTF-8

        <AppParam><id>${id}</id><value>${StringEscapeUtils.escapeXml10(value)}</value><type>${type}</type></AppParam>
        """.stripIndent().replace("\n", NEW_LINE)
    }

    /**
     * Join multiple parts to for a multipart request and return them with the corresponding header
     */
    private static ContentWithHeaders joinParts(List parts) {
        if (parts.empty) {
            new ContentWithHeaders(content: "", headers: [])
        }
        String boundary = UUID.randomUUID().toString()
        return new ContentWithHeaders(
                content: parts.collect { "--${boundary}${NEW_LINE}${it}" }.join("") + "--${boundary}--${NEW_LINE}",
                headers: [new BasicHeader(HTTP.CONTENT_TYPE, "multipart/mixed;boundary=${boundary}")] as List<Header>,
        )
    }

    static class ContentWithHeaders {
        String content
        List<Header> headers
    }

    /**
     * Prepare parent jobs is part of @prepareExtraParams
     * @param jobIds
     * @return part of parameter area
     */
    private static String prepareParentJobs(List<BEJobID> jobIds) {
        List<BEJobID> validJobIds = BEJob.uniqueValidJobIDs(jobIds)
        if (validJobIds.size() > 0) {
            String joinedParentJobs = validJobIds.collect { "done(${it})" }.join(" && ")
            return "-w \"${joinedParentJobs} \""
        } else {
            return ""
        }
    }

    /**
     * Prepare extra parameters like environment variables, resources and parent jobs
     * @param job
     * @param boundary
     * @return body for parameter area
     */
    private String prepareExtraParams(BEJob job) {
        StringBuilder envParams = new StringBuilder()

        String jointExtraParams = job.parameters.collect { key, value -> "${key}='${value}'" }.join(", ")

        if (jointExtraParams.length() > 0)
            envParams << "-env \" ${jointExtraParams} \""

        if (this.getUserEmail()) envParams << "-u ${this.getUserEmail()}"

        StringBuilder resources = new StringBuilder("-R 'select[type==any] ")
        if (job.resourceSet.isCoresSet()) {
            int cores = job.resourceSet.isCoresSet() ? job.resourceSet.getCores() : 1
            resources.append(" affinity[core(${cores})]")
        }
        resources.append("' ")
        String logging = LSFSubmissionCommand.getLoggingParameters(job.jobLog)
        String cwd = job.getWorkingDirectory() ? "-cwd ${job.getWorkingDirectory()} " : ""

        String parentJobs = ""
        if (job.parentJobIDs) {
            parentJobs = prepareParentJobs(job.parentJobIDs)
        }
        return logging + cwd + resources + envParams + StringConstants.WHITESPACE + ((ProcessingParameters) convertResourceSet(job)).processingCommandString + parentJobs
    }

    /**
     * Abort given jobs
     * @param jobs
     */
    @Override
    protected RestResult executeKillJobs(List<BEJobID> jobIDs) {
        return submitCommand("bkill ${jobIDs*.id.join(" ")}")
    }

    /**
     * Resume given jobs
     * @param jobs
     */
    @Override
    protected RestResult executeStartHeldJobs(List<BEJobID> jobIDs) {
        return submitCommand("bresume ${jobIDs*.id.join(" ")}")
    }

    /**
     * Creates a comma separated list of job ids for given jobs
     * @param jobs
     * @return comma separated list of job ids
     */
    private String prepareURLWithParam(List<BEJobID> jobIDs) {
        return BEJob.uniqueValidJobIDs(jobIDs).collect { it.id }.join(",")
    }

    /**
     * Generic method to submit any LSF valid command e.g. bstop <job id>
     * @param cmd LSF command
     */
    private RestResult submitCommand(String cmd) {
        List<Header> headers = []
        headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "application/xml "))
        headers.add(new BasicHeader("Accept", "text/plain,application/xml,text/xml,multipart/mixed"))
        String body = "<UserCmd>" +
                "<cmd>${cmd}</cmd>" +
                "</UserCmd>"
        return restExecutionService.execute(new RestSubmissionCommand(URI_USER_COMMAND, body, headers, RestSubmissionCommand.HttpMethod.HTTPPOST)) as RestResult
    }

    /**
     * Updates job information for given jobs
     * @param jobList
     */
    private Map<BEJobID, GenericJobInfo> getJobDetails(List<BEJobID> jobList) {
        List<Header> headers = []
        Map<BEJobID, GenericJobInfo> jobDetailsResult = [:]
        headers.add(new BasicHeader("Accept", "text/xml,application/xml;"))

        RestResult result = restExecutionService.execute(new RestSubmissionCommand(URI_JOB_DETAILS + prepareURLWithParam(jobList), null, headers, RestSubmissionCommand.HttpMethod.HTTPGET)) as RestResult
        if (result.isSuccessful()) {
            GPathResult res = new XmlSlurper().parseText(result.body)

            res.getProperty("job").each { NodeChild element ->
                jobDetailsResult.put(new BEJobID(element.getProperty("jobId").toString()), setJobInfoForJobDetails(element))
            }

            return jobDetailsResult

        } else {
            throw new BEException("Job details couldn't be retrieved. \n status code: ${result.statusCode} \n result: ${result.body}")
        }
    }

    /**
     * Retrieve job status for given job ids
     * @param list of job ids
     */
    @Override
    Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIds, Duration timeout = Duration.ZERO) {
        List<Header> headers = []
        headers.add(new BasicHeader("Accept", "text/xml,application/xml;"))

        RestResult result = restExecutionService.execute(new RestSubmissionCommand(URI_JOB_BASICS, null, headers, RestSubmissionCommand.HttpMethod.HTTPGET)) as RestResult
        if (result.isSuccessful()) {
            GPathResult res = new XmlSlurper().parseText(result.body)
            Map<BEJobID, JobState> resultStates = [:]
            res.getProperty("pseudoJob").each { NodeChild element ->
                String jobId = null
                if (jobIds) {
                    jobId = jobIds.find {
                        it.id.equalsIgnoreCase(element.getProperty("jobId").toString())
                    }
                } else {
                    jobId = element.getProperty("jobId").toString()
                }

                if (jobId) {
                    resultStates.put(new BEJobID(jobId), parseJobState(element.getProperty("jobStatus").toString()))
                }
            }
            return resultStates
        } else {
            throw new BEException("Job states couldn't be retrieved. \n status code: ${result.statusCode} \n result: ${result.body}")
        }
    }

    /**
     * Used by @getJobDetails to set JobInfo
     * @param job
     * @param jobDetails - XML job details
     */
    private GenericJobInfo setJobInfoForJobDetails(NodeChild jobDetails) {

        GenericJobInfo jobInfo = new GenericJobInfo(jobDetails.getProperty("jobName").toString(), new File(jobDetails.getProperty("command").toString()), new BEJobID(jobDetails.getProperty("jobId").toString()), null, null)

        String queue = jobDetails.getProperty("queue").toString()
        BufferValue swap = jobDetails.getProperty("swap") ? withCaughtAndLoggedException { new BufferValue(jobDetails.getProperty("swap").toString(), BufferUnit.m) } : null
        BufferValue memory = withCaughtAndLoggedException {
            String unit = (jobDetails.getProperty("mem") as String).find("[a-zA-Z]+")
            BufferUnit bufferUnit
            if (unit == "Gbytes")
                bufferUnit = BufferUnit.g
            else
                bufferUnit = BufferUnit.m
            jobDetails.getProperty("mem") ? new BufferValue((jobDetails.getProperty("mem") as String).find("([0-9]*[.])?[0-9]+"), bufferUnit) : null
        }
        Duration runLimit = jobDetails.getProperty("runLimit") ? withCaughtAndLoggedException { Duration.ofSeconds(Math.round(Double.parseDouble(jobDetails.getProperty("runTime").toString()))) } : null
        Integer numProcessors = withCaughtAndLoggedException { jobDetails.getProperty("numProcessors").toString() as Integer }
        Integer numberOfThreads = withCaughtAndLoggedException { jobDetails.getProperty("nthreads").toString() as Integer }
        ResourceSet usedResources = new ResourceSet(memory, numProcessors, null, runLimit, null, queue, null)
        jobInfo.setUsedResources(usedResources)

        jobInfo.setUser(jobDetails.getProperty("user").toString())
        jobInfo.setSystemTime(jobDetails.getProperty("getSystemTime").toString())
        jobInfo.setUserTime(jobDetails.getProperty("getUserTime").toString())
        jobInfo.setStartTime(withCaughtAndLoggedException { parseTime(jobDetails.getProperty("startTime").toString()) })
        jobInfo.setSubmitTime(withCaughtAndLoggedException { parseTime(jobDetails.getProperty("submitTime").toString()) })
        jobInfo.setEndTime(withCaughtAndLoggedException { parseTime(jobDetails.getProperty("endTime").toString()) })
        jobInfo.setExecutionHosts(jobDetails.getProperty("exHosts") as String ? (jobDetails.getProperty("exHosts") as String).split(":").toList() : null)
        jobInfo.setSubmissionHost(jobDetails.getProperty("fromHost").toString())
        jobInfo.setJobGroup(jobDetails.getProperty("jobGroup").toString())
        jobInfo.setDescription(jobDetails.getProperty("description").toString())
        jobInfo.setUserGroup(jobDetails.getProperty("userGroup").toString())
        jobInfo.setRunTime(jobDetails.getProperty("runTime") ? withCaughtAndLoggedException { Duration.ofSeconds(Math.round(Double.parseDouble(jobDetails.getProperty("runTime").toString()))) } : null)
        jobInfo.setProjectName(jobDetails.getProperty("projectName").toString())
        jobInfo.setExitCode(jobDetails.getProperty("exitStatus").toString() ? withCaughtAndLoggedException { Integer.valueOf(jobDetails.getProperty("exitStatus").toString()) } : null)
        jobInfo.setPidStr(jobDetails.getProperty("pidStr") as String ? (jobDetails.getProperty("pidStr") as String).split(",").toList() : null)
        jobInfo.setPgidStr(jobDetails.getProperty("pgidStr").toString())
        jobInfo.setCwd(jobDetails.getProperty("cwd").toString())
        jobInfo.setPendReason(jobDetails.getProperty("pendReason").toString())
        jobInfo.setExecCwd(jobDetails.getProperty("execCwd").toString())
        jobInfo.setPriority(jobDetails.getProperty("priority").toString())
        jobInfo.setLogFile(new File(jobDetails.getProperty("outFile").toString()))
        jobInfo.setInputFile(new File(jobDetails.getProperty("inFile").toString()))
        jobInfo.setResourceReq(jobDetails.getProperty("resReq").toString())
        jobInfo.setExecHome(jobDetails.getProperty("execHome").toString())
        jobInfo.setExecUserName(jobDetails.getProperty("execUserName").toString())
        jobInfo.setAskedHostsStr(jobDetails.getProperty("askedHostsStr").toString())

        return jobInfo
    }

    private ZonedDateTime parseTime(String str) {
        if (!str) {
            return null
        }
        ZonedDateTime.parse(str, DATE_PATTERN)
    }

    /**
     * Get the time history for each given job
     * @param jobList
     */
    private void updateJobStatistics(Map<BEJobID, GenericJobInfo> jobList) {
        List<Header> headers = []
        headers.add(new BasicHeader("Accept", "text/xml,application/xml;"))

        def result = restExecutionService.execute(new RestSubmissionCommand(URI_JOB_HISTORY + "?ids=" + prepareURLWithParam(jobList.keySet() as List), null, headers, RestSubmissionCommand.HttpMethod.HTTPGET)) as RestResult
        if (result.isSuccessful()) {
            GPathResult res = new XmlSlurper().parseText(result.body)

            res.getProperty("history").each { NodeChild jobHistory ->

                String id = ((jobHistory.getProperty("jobSummary") as GPathResult).getProperty("id")).toString()
                setJobInfoFromJobHistory(jobList.get(new BEJobID(id)), jobHistory)
            }

        } else {
            throw new BEException("Job histories couldn't be retrieved. \n status code: ${result.statusCode} \n result: ${result.body}")
        }
    }

    /**
     * Used by @updateJobStatistics to set JobInfo
     * @param job
     * @param jobHistory - xml job history
     */
    private void setJobInfoFromJobHistory(GenericJobInfo jobInfo, NodeChild jobHistory) {

        GPathResult timeSummary = jobHistory.getProperty("timeSummary") as GPathResult
        DateTimeFormatter lsfDatePattern = DateTimeFormatter.ofPattern("EEE MMM ppd HH:mm:ss yyyy").withLocale(Locale.ENGLISH)
        jobInfo.setTimeOfCalculation(withCaughtAndLoggedException { parseTime(timeSummary.getProperty("timeOfCalculation").toString()) })
        jobInfo.setTimeUserSuspState(timeSummary.getProperty("ususpTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(timeSummary.getProperty("ususpTime").toString())), 0) : null)
        jobInfo.setTimePendState(timeSummary.getProperty("pendTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(timeSummary.getProperty("pendTime").toString())), 0) : null)
        jobInfo.setTimePendSuspState(timeSummary.getProperty("psuspTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(timeSummary.getProperty("psuspTime").toString())), 0) : null)
        jobInfo.setTimeUnknownState(timeSummary.getProperty("unknownTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(timeSummary.getProperty("unknownTime").toString())), 0) : null)
        jobInfo.setTimeSystemSuspState(timeSummary.getProperty("ssuspTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(timeSummary.getProperty("ssuspTime").toString())), 0) : null)
        jobInfo.setRunTime(timeSummary.getProperty("runTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(timeSummary.getProperty("runTime").toString())), 0) : null)

    }

    @Override
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds,
                                                           Duration timeout = Duration.ZERO) {
        Map<BEJobID, GenericJobInfo> jobDetailsResult = getJobDetails(jobIds)
        updateJobStatistics(jobDetailsResult)
        return jobDetailsResult
    }

    @Override
    String getQueryJobStatesCommand() {
        return null
    }

    @Override
    String getExtendedQueryJobStatesCommand() {
        return null
    }

    @Override
    JobState parseJobState(String stateString) {
        JobState js = JobState.UNKNOWN
        if (stateString == "PENDING")
            js = JobState.QUEUED
        if (stateString == "RUNNING")
            js = JobState.RUNNING
        if (stateString == "SUSPENDED")
            js = JobState.SUSPENDED
        if (stateString == "DONE")
            js = JobState.COMPLETED_SUCCESSFUL
        if (stateString == "EXIT")
            js = JobState.FAILED

        return js
    }
}
