/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf.rest

import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.RestExecutionService
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.cluster.lsf.LSFResourceProcessingCommand
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.JobManagerCreationParameters
import de.dkfz.roddy.execution.jobs.BEJobResult
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.execution.jobs.ProcessingCommands
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.LoggerWrapper
import groovy.transform.CompileStatic
import groovy.util.slurpersupport.GPathResult
import groovy.util.slurpersupport.NodeChild
import org.apache.http.Header
import org.apache.http.message.BasicHeader
import org.apache.http.protocol.HTTP

import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * REST job manager for cluster systems.
 *
 * Created by kaercher on 22.03.17.
 */
@CompileStatic
class LSFRestJobManager extends BatchEuphoriaJobManagerAdapter {

    protected final RestExecutionService restExecutionService
    private static final LoggerWrapper logger = LoggerWrapper.getLogger(LSFRestJobManager.class.name);

    /*REST RESOURCES*/
    public static String URI_JOB_SUBMIT = "/jobs/submit"
    public static String URI_JOB_KILL = "/jobs/kill"
    public static String URI_JOB_SUSPEND = "/jobs/suspend"
    public static String URI_JOB_RESUME = "/jobs/resume"
    public static String URI_JOB_REQUEUE = "/jobs/requeue"
    public static String URI_JOB_DETAILS = "/jobs/"
    public static String URI_JOB_HISTORY = "/jobhistory"
    public static String URI_USER_COMMAND = "/userCmd"

    protected Map<String, JobState> allStates = [:]

    protected Map<String, BEJob> jobStatusListeners = [:]

    @Override
    String getSpecificJobIDIdentifier() {
        return '${LSB_JOBID}'
    }

    LSFRestJobManager(BEExecutionService restExecutionService, JobManagerCreationParameters parms) {
        super(restExecutionService, parms)
        this.restExecutionService = restExecutionService as RestExecutionService
    }


    @Override
    BEJobResult runJob(BEJob job) {
        submitJob(job)
        return job.runResult
    }


    @Override
    ProcessingCommands convertResourceSet(ResourceSet resourceSet) {
        StringBuilder resourceList = new StringBuilder()
        if (resourceSet.isQueueSet()) {
            resourceList.append(" -q ").append(resourceSet.getQueue())
        }
        if (resourceSet.isMemSet()) {
            String memo = resourceSet.getMem().toString(BufferUnit.M)
            resourceList.append(" -M ").append(memo.substring(0, memo.toString().length() - 1))
        }
        if (resourceSet.isWalltimeSet()) {
            resourceList.append(" -W ").append(durationToLSFWallTime(resourceSet.getWalltimeAsDuration()))
        }
        if (resourceSet.isCoresSet() || resourceSet.isNodesSet()) {
            int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
            resourceList.append(" -n ").append(nodes)
        }
        return new LSFResourceProcessingCommand(resourceList.toString())
    }

    private String durationToLSFWallTime(Duration wallTime) {
        if (wallTime) {
            return String.valueOf(wallTime.toMinutes())
        }
        return null
    }


    @Override
    void updateJobStatus() {

    }


    @Override
    void queryJobAbortion(List executedJobs) {
        (executedJobs as List<BEJob>).each { BEJob job -> abortJob(job) }
    }


    @Override
    Map<BEJob, JobState> queryJobStatus(List jobs, boolean forceUpdate) {
        getJobDetails(jobs)
        Map<BEJob, JobState> jobStates = [:]
        (jobs as List<BEJob>).each { BEJob job -> jobStates.put(job, job.getJobState()) }
        return jobStates
    }


    @Override
    Map<String, JobState> queryJobStatus(List jobs) {
        getJobDetails(jobs)
        Map<String, JobState> jobStates = [:]
        (jobs as List<BEJob>).each { BEJob job -> jobStates.put(job.getJobID().toString(), job.getJobState()) }
        return jobStates
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
     "Accept-Language:de-de\r\n" +
     "Content-ID: <data>\r\n" +
     "\r\n" +
     "--_Part_1_701508.1145579811786\r\n" +
     "Content-Disposition: form-data; name=\"COMMANDTORUN\"\r\n" +
     "Content-Type: application/xml; charset=UTF-8\r\n" +
     "Content-Transfer-Encoding: 8bit\r\n" +
     "Accept-Language:de-de\r\n" +
     "\r\n" +
     "<AppParam><id>COMMANDTORUN</id><value>sleep 100</value><type></type></AppParam>\r\n" +
     "--_Part_1_701508.1145579811786\r\n" +
     "Content-Disposition: form-data; name=\"EXTRA_PARAMS\"\r\n" +
     "Content-Type: application/xml; charset=UTF-8\r\n" +
     "Content-Transfer-Encoding: 8bit\r\n" +
     "Accept-Language:de-de\r\n"+
     "\r\n" +
     "<AppParam><id>EXTRA_PARAMS</id><value>-R 'select[type==any]'</value><type></type></AppParam>\r\n" +
     "--_Part_1_701508.1145579811786\r\n" +
     "\r\n" +
     "--bqJky99mlBWa-ZuqjC53mG6EzbmlxB--\r\n"
     */
    private void submitJob(BEJob job) {
        String headBoundary = UUID.randomUUID().toString()

        List<Header> headers = []
        headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "multipart/mixed;boundary=${headBoundary}"))
        headers.add(new BasicHeader("Accept", "text/xml,application/xml;"))

        StringBuilder requestBody = new StringBuilder()
        requestBody << getAppNameArea("generic", headBoundary)

        // --- Parameters Area ---
        StringBuilder paramArea = new StringBuilder()
        String bodyBoundary = UUID.randomUUID().toString()

        paramArea << prepareToolScript(job, bodyBoundary)
        paramArea << prepareJobName(job, bodyBoundary)
        paramArea << prepareExtraParams(job, bodyBoundary)

        if (paramArea.length() > 0) {
            paramArea.insert(0, getParamAreaHeader(headBoundary, bodyBoundary)) //header
            paramArea << "--${bodyBoundary}--\r\n\r\n" //footer
        }

        requestBody << paramArea
        requestBody << "--${headBoundary}--\r\n"

        logger.postAlwaysInfo("request body:\n" + requestBody)

        RestResult result = restExecutionService.execute(new RestCommand(URI_JOB_SUBMIT, requestBody.toString(), headers, RestCommand.HttpMethod.HTTPPOST)) as RestResult
        if (result.statusCode == 200) {
            logger.postAlwaysInfo("status code: " + result.statusCode + " result:" + new XmlSlurper().parseText(result.body))
            job.setRunResult(new BEJobResult(job.lastCommand, new BEJobID(new XmlSlurper().parseText(result.body).text(), job), result.successful, job.tool, job.parameters, job.parentJobs as List<BEJob>))
            job.setJobState(JobState.UNKNOWN)
            jobStatusListeners.put(job.getJobID().getId(), job)
        } else {
            job.setRunResult(new BEJobResult(job.lastCommand, null, result.successful, job.tool, job.parameters, job.parentJobs as List<BEJob>))
            logger.postAlwaysInfo("status code: " + result.statusCode + " result: " + result.body)
        }
    }

    /**
     * Get app name area for job submission
     * @param appName - usually it is "generic" which exits in all LSF environments
     * @param headBoundary - a random string
     * @return app name area string for job submission
     */
    private String getAppNameArea(String appName, String headBoundary) {
        return ["--${headBoundary}", "Content-Disposition: form-data; name=\"AppName\"",
                "Content-ID: <AppName>\r\n", "${appName}\r\n"].join("\r\n")
    }

    /**
     * Get param area header for job submission
     * @param headBoundary - a random string
     * @param bodyBoundary - a random string
     * @return param area header string for job submission
     */
    private String getParamAreaHeader(String headBoundary, String bodyBoundary) {
        return ["--${headBoundary}", "Content-Disposition: form-data; name=\"data\"",
                "Content-Type: multipart/mixed; boundary=${bodyBoundary}",
                "Accept-Language:en-en", "Content-ID: <data>", "\r\n"].join("\r\n")
    }

    /**
     * Prepare parent jobs is part of @prepareExtraParams
     * @param jobIds
     * @return part of parameter area
     */
    private String prepareParentJobs(List<BEJobID> jobIds) {
        List<BEJobID> validJobIds = BEJob.findValidJobIDs(jobIds)
        if (validJobIds.size() > 0) {
            String joinedParentJobs = validJobIds.collect { "done(${it})" }.join(" &amp;&amp; ")
            return "-w \"${joinedParentJobs} \""
        } else {
            return ""
        }
    }

    /**
     * Prepare script is part of @prepareExtraParams
     * @param job
     * @param boundary
     * @return part of parameter area
     */
    private String prepareToolScript(BEJob job, String boundary) {
        String toolScript
        if (job.getToolScript() != null && job.getToolScript().length() > 0) {
            toolScript = job.getToolScript()
        } else {
            if (job.getTool() != null) toolScript = job.getTool().getAbsolutePath()
        }
        if (toolScript) {
            return ["--${boundary}",
                    "Content-Disposition: form-data; name=\"COMMAND\"",
                    "Content-Type: application/xml; charset=UTF-8",
                    "Content-Transfer-Encoding: 8bit",
                    "Accept-Language:en-en\r\n",
                    "<AppParam><id>COMMANDTORUN</id><value>${toolScript}</value><type></type></AppParam>\r\n"].join("\r\n")
        } else {
            return ""
        }
    }

    /**
     * Prepare job name is part of @prepareExtraParams
     * @param job
     * @param boundary
     * @return part of parameter area
     */
    private String prepareJobName(BEJob job, String boundary) {
        if (job.getJobName()) {
            return ["--${boundary}",
                    "Content-Disposition: form-data; name=\"JOB_NAME\"",
                    "Content-Type: application/xml; charset=UTF-8",
                    "Content-Transfer-Encoding: 8bit",
                    "Accept-Language:en-en\r\n",
                    "<AppParam><id>JOB_NAME</id><value>${job.getJobName()}</value><type></type></AppParam>\r\n"].join("\r\n")
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
    private String prepareExtraParams(BEJob job, String boundary) {
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

        if (job.loggingDirectory) resources.append("-outdir ${job.loggingDirectory} ")

        String parentJobs = ""
        if (job.dependencyIDs) {
            parentJobs = prepareParentJobs(job.dependencyIDs)
        }
        return ["--${boundary}",
                "Content-Disposition: form-data; name=\"EXTRA_PARAMS\"",
                "Content-Type: application/xml; charset=UTF-8",
                "Content-Transfer-Encoding: 8bit",
                "Accept-Language:en-en\r\n",
                "<AppParam><id>EXTRA_PARAMS</id><value>${resources + envParams + ((LSFResourceProcessingCommand) convertResourceSet(job.resourceSet)).processingString + parentJobs}" +
                        "</value><type></type></AppParam>\r\n"].join("\r\n")
    }

    /**
     * Abort given job
     * @param job
     */
    void abortJob(BEJob job) {
        submitCommand("bkill ${job.getJobID()}")
    }

    /**
     * Suspend given job
     * @param job
     */
    void suspendJob(BEJob job) {
        submitCommand("bstop ${job.getJobID()}")
    }

    /**
     * Resume given job
     * @param job
     */
    void resumeJob(BEJob job) {
        submitCommand("bresume ${job.getJobID()}")
    }

    /**
     * Requeue given job
     * @param job
     */
    void requeueJob(BEJob job) {
        submitCommand("brequeue ${job.getJobID()}")
    }

    /**
     * Creates a comma separated list of job ids for given jobs
     * @param jobs
     * @return comma separated list of job ids
     */
    private String prepareURLWithParam(List<BEJob> jobs) {
        return BEJob.findJobsWithValidJobId(jobs).collect { it.getJobID() }.join(",")
    }

    /**
     * Generic method to submit any LSF valid command e.g. bstop <job id>
     * @param cmd LSF command
     */
    public void submitCommand(String cmd) {
        List<Header> headers = []
        headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "application/xml "))
        headers.add(new BasicHeader("Accept", "text/plain,application/xml,text/xml,multipart/mixed"))
        String body = "<UserCmd>" +
                "<cmd>${cmd}</cmd>" +
                "</UserCmd>"
        RestResult result = restExecutionService.execute(new RestCommand(URI_USER_COMMAND, body, headers, RestCommand.HttpMethod.HTTPPOST)) as RestResult
        if (result.statusCode == 200) {
            // successful
            logger.info("status code: " + result.statusCode + " result: " + result.body)
        } else {
            //error
            logger.warning("status code: " + result.statusCode + " result error2: " + result.body)
        }
    }

    /**
     * Updates job information for given jobs
     * @param jobList
     */
    void getJobDetails(List<BEJob> jobList) {
        List<Header> headers = []
        headers.add(new BasicHeader("Accept", "text/xml,application/xml;"))

        RestResult result = restExecutionService.execute(new RestCommand(URI_JOB_DETAILS + prepareURLWithParam(jobList), null, headers, RestCommand.HttpMethod.HTTPGET)) as RestResult
        if (result.statusCode == 200) {
            GPathResult res = new XmlSlurper().parseText(result.body)
            logger.info("status code: " + result.statusCode + " result:" + result.body)

            res.getProperty("job").each { NodeChild element ->
                BEJob job = jobList.find {
                    it.getJobID().toString().equalsIgnoreCase(element.getProperty("jobId").toString())
                }

                setJobInfoForJobDetails(job, element)
                job.setJobState(parseJobState(element.getProperty("jobStatus").toString()))
            }

        } else {
            logger.warning("status code: " + result.statusCode + " result: " + result.body)
        }
    }

    /**
     * Used by @getJobDetails to set JobInfo
     * @param job
     * @param jobDetails - XML job details
     */
    private void setJobInfoForJobDetails(BEJob job, NodeChild jobDetails) {
        GenericJobInfo jobInfo

        if (job.getJobInfo() != null) {
            jobInfo = job.getJobInfo()
        } else {
            jobInfo = new GenericJobInfo(jobDetails.getProperty("jobName").toString(), job.getTool(), jobDetails.getProperty("jobId").toString(), job.getParameters(), job.getDependencyIDsAsString())
        }

        String queue = jobDetails.getProperty("queue").toString()
        BufferValue swap = jobDetails.getProperty("swap") ? new BufferValue(jobDetails.getProperty("swap").toString(), BufferUnit.m) : null
        BufferValue memory = jobDetails.getProperty("mem") ? new BufferValue(jobDetails.getProperty("mem").toString(), BufferUnit.m) : null
        Duration runLimit = jobDetails.getProperty("runLimit") ? Duration.ofSeconds(Math.round(Double.parseDouble(jobDetails.getProperty("runTime").toString()))) : null
        Integer numProcessors = jobDetails.getProperty("numProcessors") as Integer
        Integer numberOfThreads = jobDetails.getProperty("nthreads") as Integer
        ResourceSet usedResources = new ResourceSet(memory, numProcessors, null, runLimit, null, queue, null)
        jobInfo.setUsedResources(usedResources)

        DateTimeFormatter lsfDatePattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withLocale(Locale.ENGLISH)
        jobInfo.setUser(jobDetails.getProperty("user").toString())
        jobInfo.setSystemTime(jobDetails.getProperty("getSystemTime").toString())
        jobInfo.setUserTime(jobDetails.getProperty("getUserTime").toString())
        jobInfo.setStartTime(!jobDetails.getProperty("startTime").toString().equals("") ? LocalDateTime.parse(jobDetails.getProperty("startTime").toString(), lsfDatePattern) : null)
        jobInfo.setSubmitTime(jobDetails.getProperty("submitTime").toString().equals("") ? LocalDateTime.parse(jobDetails.getProperty("submitTime").toString(), lsfDatePattern) : null)
        jobInfo.setEndTime(!jobDetails.getProperty("endTime").toString().equals("") ? LocalDateTime.parse(jobDetails.getProperty("endTime").toString(), lsfDatePattern) : null)
        jobInfo.setExecutionHosts(jobDetails.getProperty("exHosts").toString())
        jobInfo.setSubmissionHost(jobDetails.getProperty("fromHost").toString())
        jobInfo.setJobGroup(jobDetails.getProperty("jobGroup").toString())
        jobInfo.setDescription(jobDetails.getProperty("description").toString())
        jobInfo.setUserGroup(jobDetails.getProperty("userGroup").toString())
        jobInfo.setRunTime(jobDetails.getProperty("runTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(jobDetails.getProperty("runTime").toString()))) : null)
        jobInfo.setProjectName(jobDetails.getProperty("projectName").toString())
        jobInfo.setExitCode(jobDetails.getProperty("exitStatus").toString() ? Integer.valueOf(jobDetails.getProperty("exitStatus").toString()) : null)
        jobInfo.setPidStr(jobDetails.getProperty("pidStr").toString())
        jobInfo.setPgidStr(jobDetails.getProperty("pgidStr").toString())
        jobInfo.setCwd(jobDetails.getProperty("cwd").toString())
        jobInfo.setPendReason(jobDetails.getProperty("pendReason").toString())
        jobInfo.setExecCwd(jobDetails.getProperty("execCwd").toString())
        jobInfo.setPriority(jobDetails.getProperty("priority").toString())
        jobInfo.setOutFile(jobDetails.getProperty("outFile").toString())
        jobInfo.setInFile(jobDetails.getProperty("inFile").toString())
        jobInfo.setResourceReq(jobDetails.getProperty("resReq").toString())
        jobInfo.setExecHome(jobDetails.getProperty("execHome").toString())
        jobInfo.setExecUserName(jobDetails.getProperty("execUserName").toString())
        jobInfo.setAskedHostsStr(jobDetails.getProperty("askedHostsStr").toString())
        job.setJobInfo(jobInfo)
    }

    /**
     * Get the time history for each given job
     * @param jobList
     */
    void updateJobStatistics(List<BEJob> jobList) {
        List<Header> headers = []
        headers.add(new BasicHeader("Accept", "text/xml,application/xml;"))

        def result = restExecutionService.execute(new RestCommand(URI_JOB_HISTORY + "?ids=" + prepareURLWithParam(jobList), null, headers, RestCommand.HttpMethod.HTTPGET)) as RestResult
        if (result.statusCode == 200) {
            GPathResult res = new XmlSlurper().parseText(result.body)
            logger.info("status code: " + result.statusCode + " result:" + result.body)

            res.getProperty("history").each { NodeChild jobHistory ->

                BEJob job = jobList.find {
                    it.getJobID().toString().equalsIgnoreCase((jobHistory.getProperty("jobSummary") as GPathResult).getProperty("id").toString())
                }

                setJobInfoForJobHistory(job, jobHistory)
            }

        } else {
            logger.warning("status code: " + result.statusCode + " result: " + result.body)
        }
    }

    /**
     * Used by @updateJobStatistics to set JobInfo
     * @param job
     * @param jobHistory - xml job history
     */
    void setJobInfoForJobHistory(BEJob job, NodeChild jobHistory) {
        GenericJobInfo jobInfo

        if (job.getJobInfo() != null)
            jobInfo = job.getJobInfo()
        else
            jobInfo = new GenericJobInfo((jobHistory.getProperty("jobSummary") as GPathResult).getProperty("jobName").toString(), job.getTool(), (jobHistory.getProperty("jobSummary") as GPathResult).getProperty("id").toString(), job.getParameters(), job.getDependencyIDsAsString())
        GPathResult timeSummary = jobHistory.getProperty("timeSummary") as GPathResult
        DateTimeFormatter lsfDatePattern = DateTimeFormatter.ofPattern("EEE MMM ppd HH:mm:ss yyyy").withLocale(Locale.ENGLISH)
        println timeSummary.getProperty("timeOfCalculation")
        println timeSummary.getProperty("ususpTime")
        println timeSummary.getProperty("pendTime")
        println timeSummary.getProperty("psuspTime")
        println timeSummary.getProperty("runTime")

        jobInfo.setTimeOfCalculation(timeSummary.getProperty("timeOfCalculation") ? LocalDateTime.parse(timeSummary.getProperty("timeOfCalculation").toString() + " " + LocalDateTime.now().getYear(), lsfDatePattern) : null)
        jobInfo.setTimeUserSuspState(timeSummary.getProperty("ususpTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(timeSummary.getProperty("ususpTime").toString())), 0) : null)
        jobInfo.setTimePendState(timeSummary.getProperty("pendTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(timeSummary.getProperty("pendTime").toString())), 0) : null)
        jobInfo.setTimePendSuspState(timeSummary.getProperty("psuspTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(timeSummary.getProperty("psuspTime").toString())), 0) : null)
        jobInfo.setTimeUnknownState(timeSummary.getProperty("unknownTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(timeSummary.getProperty("unknownTime").toString())), 0) : null)
        jobInfo.setTimeSystemSuspState(timeSummary.getProperty("ssuspTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(timeSummary.getProperty("ssuspTime").toString())), 0) : null)
        jobInfo.setRunTime(timeSummary.getProperty("runTime") ? Duration.ofSeconds(Math.round(Double.parseDouble(timeSummary.getProperty("runTime").toString())), 0) : null)
        job.setJobInfo(jobInfo)
    }

    @Override
    void addJobStatusChangeListener(BEJob job) {
        synchronized (jobStatusListeners) {
            jobStatusListeners.put(job.getJobID().getId(), job)
        }
    }

    @Override
    Map<String, JobState> queryJobStatusById(List<String> jobIds, boolean forceUpdate = false) {
        List<BEJob> beJobs = []
        for (String id : jobIds) {
            if (jobStatusListeners.get(id))
                beJobs.add(jobStatusListeners.get(id))
        }
        getJobDetails(beJobs)
        Map<String, JobState> jobStates = [:]
        (beJobs as List<BEJob>).each { BEJob job -> jobStates.put(job.getJobID().toString(), job.getJobState()) }
        return jobStates
    }

    @Override
    Map<String, JobState> queryJobStatusAll(boolean forceUpdate = false) {
        List<BEJob> jobs = jobStatusListeners.values().collect()
        getJobDetails(jobs)
        Map<String, JobState> jobStates = [:]
        (jobs as List<BEJob>).each { BEJob job -> jobStates.put(job.getJobID().toString(), job.getJobState()) }
        return jobStates
    }


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
