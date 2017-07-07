/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.cluster.lsf.rest

import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.RestExecutionService
import de.dkfz.roddy.execution.cluster.pbs.PBSJobDependencyID
import de.dkfz.roddy.execution.cluster.pbs.PBSResourceProcessingCommand
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.JobManagerCreationParameters
import de.dkfz.roddy.execution.jobs.JobResult
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.execution.jobs.ProcessingCommands
import de.dkfz.roddy.tools.BufferUnit
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


    LSFRestJobManager(BEExecutionService restExecutionService, JobManagerCreationParameters parms) {
        super(restExecutionService, parms)
        this.restExecutionService = restExecutionService as RestExecutionService
    }


    @Override
    de.dkfz.roddy.execution.jobs.JobResult runJob(BEJob job) {
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
            String memo = resourceSet.getMem().toString(BufferUnit.K)
            resourceList.append(" -M ").append(memo.substring(0, memo.toString().length() - 1))
        }
        if (resourceSet.isWalltimeSet()) {
            resourceList.append(" -W ").append(resourceSet.getWalltime().toString().substring(6, resourceSet.getWalltime().toString().length()))
        }
        if (resourceSet.isCoresSet() || resourceSet.isNodesSet()) {
            int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
            int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1
            resourceList.append(" -n ").append(nodes * cores)
        }
        return new PBSResourceProcessingCommand(resourceList.toString())
    }


    @Override
    void updateJobStatus() {

    }


    @Override
    void queryJobAbortion(List executedJobs) {
        (executedJobs as List<BEJob>).each { BEJob job -> abortJob(job) }
    }


    @Override
    Map<BEJob, JobState> queryJobStatus(List list, boolean forceUpdate) {
        getJobDetails(list)
        Map<BEJob, JobState> jobStates = [:]
        (list as List<BEJob>).each { BEJob job -> jobStates.put(job, job.getJobState()) }
        return jobStates
    }


    @Override
    Map<String, JobState> queryJobStatus(List jobIDs) {
        getJobDetails(jobIDs)
        Map<String, JobState> jobStates = [:]
        (jobIDs as List<BEJob>).each { BEJob job -> jobStates.put(job.getJobID(), job.getJobState()) }
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
            job.setRunResult(new JobResult(job.lastCommand, new PBSJobDependencyID(job, new XmlSlurper().parseText(result.body).text()), true, job.tool, job.parameters, job.parentJobs as List<BEJob>))
        } else {
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
                "Accept-Language:de-de", "Content-ID: <data>", "\r\n"].join("\r\n")
    }

    /**
     * Prepare parent jobs is part of @prepareExtraParams
     * @param jobs
     * @return part of parameter area
     */
    private String prepareParentJobs(List<BEJob> jobs) {
        String joinedParentJobs = jobs.collect { "done(${it.getJobID()})" }.join(" &amp;&amp; ")
        if (joinedParentJobs.length() > 0)
            return "-w \"${joinedParentJobs} \""

        return ""
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
                    "Accept-Language:en-en",
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

        String jointExraParams = job.parameters.collect { key, value -> "${key}='${value}'" }.join(", ")

        if (jointExraParams.length() > 0)
            envParams << "-env \" ${jointExraParams} \""

        if (this.getUserEmail()) envParams << "-u ${this.getUserEmail()}"


        String parentJobs = ""
        if (job.getParentJobs() != null && job.getParentJobs()?.size()) {
            parentJobs = prepareParentJobs(job.getParentJobs() as List<BEJob>)
        }
        return ["--${boundary}",
                "Content-Disposition: form-data; name=\"EXTRA_PARAMS\"",
                "Content-Type: application/xml; charset=UTF-8",
                "Content-Transfer-Encoding: 8bit",
                "Accept-Language:en-en\r\n",
                "<AppParam><id>EXTRA_PARAMS</id><value>${"-R 'select[type==any]' " + envParams + ((PBSResourceProcessingCommand) convertResourceSet(job.resourceSet)).processingString + parentJobs}" +
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
        return jobs.collect { it.getJobID() }.join(",")
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
                BEJob job = jobList.find { it.getJobID().equalsIgnoreCase(element.getProperty("jobId").toString()) }

                setJobInfoForJobDetails(job, element)

                if (element.getProperty("jobStatus").toString() == "PENDING")
                    job.setJobState(JobState.QUEUED)
                if (element.getProperty("jobStatus").toString() == "RUNNING")
                    job.setJobState(JobState.RUNNING)
                if (element.getProperty("jobStatus").toString() == "SUSPENDED")
                    job.setJobState(JobState.ABORTED)
                if (element.getProperty("jobStatus").toString() == "DONE")
                    job.setJobState(JobState.OK)
                if (element.getProperty("jobStatus").toString() == "EXIT")
                    job.setJobState(JobState.OK)
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
    void setJobInfoForJobDetails(BEJob job, NodeChild jobDetails) {
        GenericJobInfo jobInfo

        if (job.getJobInfo() != null) {
            jobInfo = job.getJobInfo()
        } else {
            jobInfo = new GenericJobInfo(jobDetails.getProperty("jobName").toString(), job.getTool(), jobDetails.getProperty("jobId").toString(), job.getParameters(), job.getDependencyIDsAsString())
        }

        Map<String, String> jobInfoProperties = [:]
        Map<String, String> jobDetailsAttributes = jobDetails.attributes()
        jobDetailsAttributes.each { String key, value ->
            jobInfoProperties.put(key, jobDetails.getProperty(key).toString())
        }
        DateTimeFormatter lsfDatePattern = DateTimeFormatter.ofPattern("EEE MMM dd hh:mm:ss yyyy")

        jobInfo.setUser(jobInfoProperties.get("user"))
        jobInfo.setCpuTime(Duration.parse(jobInfoProperties.get("cpuTime")))
        jobInfo.setSystemTime(jobInfoProperties.get("getSystemTime"))
        jobInfo.setUserTime(jobInfoProperties.get("getUserTime"))
        jobInfo.setStartTime(LocalDateTime.parse(jobInfoProperties.get("startTime"), lsfDatePattern))
        jobInfo.setSubTime(LocalDateTime.parse(jobInfoProperties.get("submitTime"), lsfDatePattern))
        jobInfo.setEndTime(LocalDateTime.parse(jobInfoProperties.get("endTime"), lsfDatePattern))
        jobInfo.setQueue(jobInfoProperties.get("queue"))
        jobInfo.setExHosts(jobInfoProperties.get("exHosts"))
        jobInfo.setSubHost(jobInfoProperties.get("fromHost"))
        jobInfo.setJobGroup(jobInfoProperties.get("jobGroup"))
        jobInfo.setSwap(jobInfoProperties.get("swap"))
        jobInfo.setDescription(jobInfoProperties.get("description"))
        jobInfo.setUserGroup(jobInfoProperties.get("userGroup"))
        jobInfo.setMaxMemory(jobInfoProperties.get("mem").toInteger())
        jobInfo.setRunTime(Duration.parse(jobInfoProperties.get("runTime")))
        jobInfo.setRunLimit(jobInfoProperties.get("runLimit"))
        jobInfo.setNumProcessors(jobInfoProperties.get("numProcessors"))
        jobInfo.setNthreads(jobInfoProperties.get("nthreads"))
        jobInfo.setProjectName(jobInfoProperties.get("projectName"))
        jobInfo.setExitStatus(jobInfoProperties.get("exitStatus"))
        jobInfo.setPidStr(jobInfoProperties.get("pidStr"))
        jobInfo.setPgidStr(jobInfoProperties.get("pgidStr"))
        jobInfo.setCwd(jobInfoProperties.get("cwd"))
        jobInfo.setPendReason(jobInfoProperties.get("pendReason"))
        jobInfo.setExecCwd(jobInfoProperties.get("execCwd"))
        jobInfo.setPriority(jobInfoProperties.get("priority"))
        jobInfo.setOutFile(jobInfoProperties.get("outFile"))
        jobInfo.setInFile(jobInfoProperties.get("inFile"))
        jobInfo.setResReq(jobInfoProperties.get("resReq"))
        jobInfo.setExecHome(jobInfoProperties.get("execHome"))
        jobInfo.setExecUserName(jobInfoProperties.get("execUserName"))
        jobInfo.setAskedHostsStr(jobInfoProperties.get("askedHostsStr"))
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
                    it.getJobID().equalsIgnoreCase((jobHistory.getProperty("jobSummary") as GPathResult).getProperty("id").toString())
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
        jobInfo.setTimeOfCalculation(timeSummary.getProperty("timeOfCalculation").toString())
        jobInfo.setTimeUserSuspState(timeSummary.getProperty("ususpTime").toString())
        jobInfo.setTimePendState(timeSummary.getProperty("pendTime").toString())
        jobInfo.setTimePendSuspState(timeSummary.getProperty("psuspTime").toString())
        jobInfo.setTimeUnknownState(timeSummary.getProperty("unknownTime").toString())
        jobInfo.setTimeSystemSuspState(timeSummary.getProperty("ssuspTime").toString())
        jobInfo.setRunTime(Duration.parse(timeSummary.getProperty("runTime").toString()))
        job.setJobInfo(jobInfo)
    }


}
