/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest

import de.dkfz.eilslabs.batcheuphoria.config.ResourceSet
import de.dkfz.eilslabs.batcheuphoria.execution.ExecutionService
import de.dkfz.eilslabs.batcheuphoria.execution.cluster.pbs.PBSJobDependencyID
import de.dkfz.eilslabs.batcheuphoria.execution.cluster.pbs.PBSResourceProcessingCommand
import de.dkfz.eilslabs.batcheuphoria.jobs.GenericJobInfo
import de.dkfz.eilslabs.batcheuphoria.jobs.Job
import de.dkfz.eilslabs.batcheuphoria.jobs.JobManagerCreationParameters
import de.dkfz.eilslabs.batcheuphoria.jobs.JobResult
import de.dkfz.eilslabs.batcheuphoria.jobs.JobState
import de.dkfz.eilslabs.batcheuphoria.jobs.ProcessingCommands
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.LoggerWrapper
import groovy.util.slurpersupport.GPathResult
import groovy.util.slurpersupport.NodeChild
import org.apache.http.Header
import org.apache.http.message.BasicHeader
import org.apache.http.protocol.HTTP

/**
 * REST job manager for cluster systems.
 *
 * Created by kaercher on 22.03.17.
 */
class RestJobManager extends JobManagerAdapter
 {

     protected final ExecutionService restExecutionService
     private static final LoggerWrapper logger = LoggerWrapper.getLogger(RestJobManager.class.name);


     /*REST RESOURCES*/
     public static String URI_JOB_SUBMIT = "jobs/submit"
     public static String URI_JOB_KILL = "jobs/kill"
     public static String URI_JOB_SUSPEND = "jobs/suspend"
     public static String URI_JOB_RESUME = "jobs/resume"
     public static String URI_JOB_REQUEUE = "jobs/requeue"
     public static String URI_JOB_DETAILS = "jobs/"
     public static String URI_JOB_HISTORY = "jobhistory"
     public static String URI_USER_COMMAND = "userCmd"


     RestJobManager(ExecutionService restExecutionService, JobManagerCreationParameters parms){
         super(restExecutionService, parms)
         this.restExecutionService = restExecutionService
     }


     @Override
     de.dkfz.roddy.execution.jobs.JobResult runJob(Job job) {
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
             resourceList.append(" -M ").append(memo.substring(0,memo.toString().length()-1))
         }
         if (resourceSet.isWalltimeSet()) {
             resourceList.append(" -W ").append(resourceSet.getWalltime().toString().substring(6,resourceSet.getWalltime().toString().length()))
         }
         if (resourceSet.isCoresSet() || resourceSet.isNodesSet()) {
             int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
             int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1
             resourceList.append(" -n ").append(nodes*cores)
         }
         return new PBSResourceProcessingCommand(resourceList.toString())
     }



     @Override
     void updateJobStatus() {

     }


     @Override
     void queryJobAbortion(List executedJobs) {
         executedJobs.each {Job job -> abortJob(job)}
     }


     @Override
     Map<Job, JobState> queryJobStatus(List list, boolean forceUpdate) {
         getJobDetails(list)
         Map<Job, JobState> jobStates = [:]
         list.each {Job job -> jobStates.put(job,job.getJobState())}
         return queryJobStatus(list)
     }


     @Override
     Map<String, JobState> queryJobStatus(List jobIDs) {
         getJobDetails(jobIDs)
         Map<String, JobState> jobStates = [:]
         jobIDs.each {Job job -> jobStates.put(job.getJobID(),job.getJobState())}
         return  jobStates
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
     private void submitJob(Job job){
         String headBoundary= UUID.randomUUID().toString()

         List<Header> headers = []
         headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "multipart/mixed;boundary=${headBoundary}"))
         headers.add(new BasicHeader("Accept", "text/xml,application/xml;"))

         StringBuilder requestBody = new StringBuilder()
         requestBody << getAppNameArea("generic", headBoundary)

         // --- Parameters Area ---
         StringBuilder paramArea = new StringBuilder()
         String bodyBoundary = UUID.randomUUID().toString()

         paramArea << prepareToolScript(job,bodyBoundary)
         paramArea << prepareJobName(job,bodyBoundary)
         paramArea << prepareExtraParams(job,bodyBoundary)

         if(paramArea.length() > 0) {
             paramArea.insert(0,getParamAreaHeader(headBoundary,bodyBoundary)) //header
             paramArea << "--${bodyBoundary}--\r\n\r\n" //footer
         }

         requestBody << paramArea
         requestBody << "--${headBoundary}--\r\n"

         logger.postAlwaysInfo("request body:\n"+requestBody)

         RestResult result = restExecutionService.execute(new RestCommand(URI_JOB_SUBMIT, requestBody.toString(), headers, RestCommand.HttpMethod.HTTPPOST))
         if(result.statusCode == 200){
             logger.postAlwaysInfo("status code: " + result.statusCode + " result:" + new XmlSlurper().parseText(result.body))
             job.setRunResult(new JobResult(job.lastCommand, new PBSJobDependencyID(job, new XmlSlurper().parseText(result.body).text()), true, job.tool, job.parameters, job.parentJobs))
         }else{
             logger.postAlwaysInfo("status code: "+result.statusCode+" result: "+result.body)
         }
     }


     /**
      * Get app name area for job submission
      * @param appName - usually it is "generic" which exits in all LSF environments
      * @param headBoundary - a random string
      * @return app name area string for job submission
      */
     private String getAppNameArea(String appName, String headBoundary){
         return "--${headBoundary}\r\n" +
                 "Content-Disposition: form-data; name=\"AppName\"\r\n" +
                 "Content-ID: <AppName>\r\n" +
                 "\r\n" +
                 "${appName}\r\n"
     }


     /**
      * Get param area header for job submission
      * @param headBoundary - a random string
      * @param bodyBoundary - a random string
      * @return param area header string for job submission
      */
     private String getParamAreaHeader(String headBoundary, String bodyBoundary){
         return "--${headBoundary}\r\n" +
                 "Content-Disposition: form-data; name=\"data\"\r\n" +
                 "Content-Type: multipart/mixed; boundary=${bodyBoundary}\r\n" +
                 "Accept-Language:de-de\r\n" +
                 "Content-ID: <data>\r\n" +
                 "\r\n"
     }


     /**
      * Prepare parent jobs is part of @prepareExtraParams
      * @param jobs
      * @return part of parameter area
      */
     private String prepareParentJobs(List<Job> jobs){
         StringBuilder parentJobs = new StringBuilder()
         jobs.eachWithIndex {Job jobTemp, index ->
             if(index == 0)
                 parentJobs << " -w \"ended(${jobTemp.getJobID()})"
             else
                 parentJobs << " &amp;&amp; ended(${jobTemp.getJobID()})"
         }
         if(parentJobs.length() > 0)
             parentJobs << "\""

         return parentJobs
     }


     /**
      * Prepare script is part of @prepareExtraParams
      * @param job
      * @param boundary
      * @return part of parameter area
      */
     private String prepareToolScript(Job job, String boundary){
         String toolScript
         if(job.getToolScript() != null && job.getToolScript().length() > 0) {
             toolScript = job.getToolScript()
         }else{
             if(job.getTool() != null) toolScript = job.getTool().getAbsolutePath()
         }
         if(toolScript){
             return  "--${boundary}\r\n" +
                     "Content-Disposition: form-data; name=\"COMMAND\"\r\n" +
                     "Content-Type: application/xml; charset=UTF-8\r\n" +
                     "Content-Transfer-Encoding: 8bit\r\n" +
                     "Accept-Language:de-de\r\n" +
                     "\r\n" +
                     "<AppParam><id>COMMANDTORUN</id><value>${toolScript}</value><type></type></AppParam>\r\n"
         }else{
             return ""
         }
     }


     /**
      * Prepare job name is part of @prepareExtraParams
      * @param job
      * @param boundary
      * @return part of parameter area
      */
     private String prepareJobName(Job job, String boundary){
         if(job.getJobName()){
             return  "--${boundary}\r\n" +
                     "Content-Disposition: form-data; name=\"JOB_NAME\"\r\n" +
                     "Content-Type: application/xml; charset=UTF-8\r\n" +
                     "Content-Transfer-Encoding: 8bit\r\n" +
                     "Accept-Language:de-de\r\n" +
                     "\r\n" +
                     "<AppParam><id>JOB_NAME</id><value>${job.getJobName()}</value><type></type></AppParam>\r\n"
         }else{
             return ""
         }
     }


     /**
      * Prepare extra parameters like environment variables, resources and parent jobs
      * @param job
      * @param boundary
      * @return body for parameter area
      */
     private String prepareExtraParams(Job job, String boundary){
         StringBuilder envParams = new StringBuilder()
         //if (this.getUserGroup()) envParams << "-G ${this.getUserGroup()}"

         job.parameters.eachWithIndex { key, value, index ->
             if (index == 0)
                 envParams << "-env \" ${key}='${value}'"
             else
                 envParams << "," + "${key}='${value}'"
         }

         if(envParams.length() > 0)
             envParams << "\""

         if (this.getUserEmail()) envParams << "-u ${this.getUserEmail()}"


         String parentJobs =""
         if(job.getParentJobs() != null && job.getParentJobs()?.size()){
             parentJobs = prepareParentJobs(job.getParentJobs())
         }
         return  "--${boundary}\r\n" +
                 "Content-Disposition: form-data; name=\"EXTRA_PARAMS\"\r\n" +
                 "Content-Type: application/xml; charset=UTF-8\r\n" +
                 "Content-Transfer-Encoding: 8bit\r\n" +
                 "Accept-Language:de-de\r\n" +
                 "\r\n" +
                 "<AppParam><id>EXTRA_PARAMS</id><value>${"-R 'select[type==any]' "+envParams+((PBSResourceProcessingCommand)convertResourceSet(job.resourceSet)).processingString+parentJobs}" +
                 "</value><type></type></AppParam>\r\n"
     }


     /**
      * Abort given job
      * @param job
      */
     void abortJob(Job job){
         submitCommand("bkill ${job.getJobID()}")
     }


     /**
      * Suspend given job
      * @param job
      */
     void suspendJob(Job job){
         submitCommand("bstop ${job.getJobID()}")
     }


     /**
      * Resume given job
      * @param job
      */
     void resumeJob(Job job){
         submitCommand("bresume ${job.getJobID()}")
     }


     /**
      * Requeue given job
      * @param job
      */
     void requeueJob(Job job){
         submitCommand("brequeue ${job.getJobID()}")
     }


     /**
      * Creates a comma separated list of job ids for given jobs
      * @param jobs
      * @return comma separated list of job ids
      */
     private String prepareURLWithParam(List<Job> jobs){
         String jobIds=""
         jobs.eachWithIndex {job, index ->
             if (index == 0){
                 jobIds= job.getJobID()
             }else{
                 jobIds << ","+job.getJobID()
             }
         }
         return jobIds
    }


     /**
      * Generic method to submit any LSF valid command e.g. bstop <job id>
      * @param cmd LSF command
      */
     public void submitCommand(String cmd){
         List<Header> headers = []
         headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "application/xml "))
         headers.add(new BasicHeader("Accept", "text/plain,application/xml,text/xml,multipart/mixed"))
         String body = "<UserCmd>" +
                 "<cmd>${cmd}</cmd>" +
                 "</UserCmd>"
         def result =restExecutionService.execute(new RestCommand(URI_USER_COMMAND,body,headers,RestCommand.HttpMethod.HTTPPOST))
         if(result.statusCode == 200){
             // successful
             logger.info("status code: "+result.statusCode+" result: "+result.body)
         }else{
             //error
             logger.warning("status code: " + result.statusCode + " result error2: " + result.body)
         }
    }


     /**
      * Updates job information for given jobs
      * @param jobList
      */
     void getJobDetails(List<Job> jobList){
         List<Header> headers = []
         headers.add(new BasicHeader("Accept", "text/xml,application/xml;"))

         def result =restExecutionService.execute(new RestCommand(URI_JOB_DETAILS+prepareURLWithParam(jobList),null,headers,RestCommand.HttpMethod.HTTPGET))
         if(result.statusCode == 200){
             GPathResult res = new XmlSlurper().parseText(result.body)
             logger.info("status code: " + result.statusCode + " result:" + result.body)

             res.job.each {NodeChild element ->
                 Job job = jobList.find {it.getJobID().equalsIgnoreCase(element.jobId.text()) }

                 setJobInfoForJobDetails(job,element)

                 if(element.jobStatus.text() == "PENDING")
                     job.setJobState(JobState.QUEUED)
                 if(element.jobStatus.text() == "RUNNING")
                    job.setJobState(JobState.RUNNING)
                 if(element.jobStatus.text() == "SUSPENDED")
                     job.setJobState(JobState.ABORTED)
                 if(element.jobStatus.text() == "DONE")
                     job.setJobState(JobState.OK)
                 if(element.jobStatus.text() == "EXIT")
                     job.setJobState(JobState.OK)
             }

         }else{
             logger.warning("status code: " + result.statusCode + " result: " + result.body)
         }
     }


     /**
      * Used by @getJobDetails to set JobInfo
      * @param job
      * @param jobDetails - XML job details
      */
     void setJobInfoForJobDetails(Job job, NodeChild jobDetails){
         GenericJobInfo jobInfo

         if(job.getJobInfo() != null)
             jobInfo = job.getJobInfo()
         else
             jobInfo = new GenericJobInfo(jobDetails.jobName.text(),job.getTool(),jobDetails.jobId.text(),job.getParameters(),job.getParentJobs())

         jobInfo.setUser(jobDetails.user.text())
         jobInfo.setCpuTime(jobDetails.cpuTime.text())
         jobInfo.setSystemTime(jobDetails.getSystemTime.text())
         jobInfo.setUserTime(jobDetails.getUserTime.text())
         jobInfo.setStartTimeGMT(jobDetails.startTime.text())
         jobInfo.setSubTimeGMT(jobDetails.submitTime.text())
         jobInfo.setEndTimeGMT(jobDetails.endTime.text())
         jobInfo.setQueue(jobDetails.queue.text())
         jobInfo.setExHosts(jobDetails.exHosts.text())
         jobInfo.setSubHost(jobDetails.fromHost.text())
         jobInfo.setJobGroup(jobDetails.jobGroup.text())
         jobInfo.setSwap(jobDetails.swap.text())
         jobInfo.setDescription(jobDetails.description.text())
         jobInfo.setUserGroup(jobDetails.userGroup.text())
         jobInfo.setMemory(jobDetails.mem.text().toInteger())
         jobInfo.setRunTime(jobDetails.runTime.text())
         jobInfo.setRunLimit(jobDetails.runLimit.text())
         jobInfo.setNumProcessors(jobDetails.numProcessors.text())
         jobInfo.setNthreads(jobDetails.nthreads.text())
         jobInfo.setProjectName(jobDetails.projectName.text())
         jobInfo.setExitStatus(jobDetails.exitStatus.text())
         jobInfo.setPidStr(jobDetails.pidStr.text())
         jobInfo.setPgidStr(jobDetails.pgidStr.text())
         jobInfo.setCwd(jobDetails.cwd.text())
         jobInfo.setPendReason(jobDetails.pendReason.text())
         jobInfo.setExecCwd(jobDetails.execCwd.text())
         jobInfo.setPriority(jobDetails.priority.text())
         jobInfo.setOutfile(jobDetails.outfile.text())
         jobInfo.setInfile(jobDetails.infile.text())
         jobInfo.setResReq(jobDetails.resReq.text())
         jobInfo.setExecHome(jobDetails.execHome.text())
         jobInfo.setExecUserName(jobDetails.execUserName.text())
         jobInfo.setAskedHostsStr(jobDetails.askedHostsStr.text())
         job.setJobInfo(jobInfo)
     }


     /**
      * Get the time history for each given job
      * @param jobList
      */
     void getJobHistory(List<Job> jobList){
         List<Header> headers = []
         headers.add(new BasicHeader("Accept", "text/xml,application/xml;"))

         def result =restExecutionService.execute(new RestCommand(URI_JOB_HISTORY+"?ids="+prepareURLWithParam(jobList),null,headers,RestCommand.HttpMethod.HTTPGET))
         if(result.statusCode == 200){
             GPathResult res = new XmlSlurper().parseText(result.body)
             logger.info("status code: "+result.statusCode+" result:"+result.body)

             res.history.each {NodeChild jobHistory ->

                 Job job = jobList.find {it.getJobID().equalsIgnoreCase(jobHistory.jobSummary.id.text()) }

                 setJobInfoForJobHistory(job,jobHistory)
             }

        }else{
            logger.warning("status code: " + result.statusCode + " result: " + result.body)
        }
     }


     /**
      * Used by @getJobHistory to set JobInfo
      * @param job
      * @param jobHistory - xml job history
      */
     void setJobInfoForJobHistory(Job job, NodeChild jobHistory){
         GenericJobInfo jobInfo

         if(job.getJobInfo() != null)
             jobInfo = job.getJobInfo()
         else
             jobInfo = new GenericJobInfo(jobHistory.jobSummary.jobName.text(),job.getTool(),jobHistory.jobSummary.id.text(),job.getParameters(), job.getParentJobs())

         jobInfo.setTimeOfCalculation(jobHistory.timeSummary.timeOfCalculation.text())
         jobInfo.setTimeUserSuspState(jobHistory.timeSummary.ususpTime.text())
         jobInfo.setTimePendState(jobHistory.timeSummary.pendTime.text())
         jobInfo.setTimePendSuspState(jobHistory.timeSummary.psuspTime.text())
         jobInfo.setTimeUnknownState(jobHistory.timeSummary.unknownTime.text())
         jobInfo.setTimeSystemSuspState(jobHistory.timeSummary.ssuspTime.text())
         jobInfo.setRunTime(jobHistory.timeSummary.runTime.text())
         job.setJobInfo(jobInfo)
     }


}
