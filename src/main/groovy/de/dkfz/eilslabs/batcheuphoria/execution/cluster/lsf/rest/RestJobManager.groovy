/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest

import de.dkfz.eilslabs.batcheuphoria.execution.RestExecutionService
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
import org.apache.http.Header
import org.apache.http.message.BasicHeader
import org.apache.http.protocol.HTTP

/**
 * Factory for the management of LSF cluster systems.
 *
 *
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



    public static final String COMMANDTORUN = "Command to run"
    public static final String EXTRA_PARAMS ="Other bsub op-tions"
    public static final String MAX_MEM ="Maximum memory size per pro-cess"
    public static final String MAX_NUM_CPU = ""
    public static final String MIN_NUM_CPU = ""
    public static final String PROC_PRE_HOST = ""
    public static final String EXTRA_RES = ""
    public static final String RUNLIMITMINUTE = ""
    public static final String RERUNABLE = ""
    public static final String JOB_NAME = ""
    public static final String APP_PROFILE = ""
    public static final String PRJ_NAME = ""
    public static final String RES_ID = ""
    public static final String LOGIN_SHELL = ""
    public static final String QUEUE = ""
    public static final String INPUT_FILE = ""
    public static final String OUTPUT_FILE = ""
    public static final String ERROR_FILE = ""



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
     de.dkfz.roddy.execution.jobs.JobResult runJob(Job job, boolean runDummy) {
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

     boolean compareJobIDs(String jobID, String id) {
         if (jobID.length() == id.length()) {
             return jobID == id
         } else {
             String id0 = jobID.split("[.]")[0]
             String id1 = id.split("[.]")[0]
             return id0 == id1
         }
     }

     @Override
     void queryJobAbortion(List executedJobs) {
         abortJob(executedJobs)

     }

     @Override
     Map<Job, JobState> queryJobStatus(List list, boolean forceUpdate) {
         getJobdetails(list)
         Map<Job, JobState> jobStates = [:]
         list.each {Job job -> jobStates.put(job,job.getJobState())}
         return queryJobStatus(list)
     }

     @Override
     Map<String, JobState> queryJobStatus(List jobIDs) {
         getJobdetails(jobIDs)
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
         String resource = URI_JOB_SUBMIT
         String boundary='bqJky99mlBWa-ZuqjC53mG6EzbmlxB'
         List<Header> headers = []
         headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "multipart/mixed;boundary=${boundary}"))
         headers.add(new BasicHeader("Accept", "text/xml,application/xml;"))

         String appName = "generic"
         String body

         // --- Application Name Area  ----
         String appNameArea = "--${boundary}\r\n" +
                 "Content-Disposition: form-data; name=\"AppName\"\r\n" +
                 "Content-ID: <AppName>\r\n" +
                 "\r\n" +
                 "${appName}\r\n"

         body = appNameArea

         // --- Parameters Area ---
         if(job.parameters.size() != 0){

            String newBoundary = '_Part_1_701508.1145579811786'
            String paramArea = "--${boundary}\r\n" +
                    "Content-Disposition: form-data; name=\"data\"\r\n" +
                    "Content-Type: multipart/mixed; boundary=${newBoundary}\r\n" +
                    "Accept-Language:de-de\r\n" +
                    "Content-ID: <data>\r\n" +
                    "\r\n"

            String envParams = ""
            job.parameters.eachWithIndex { key, value, index ->
                if (index == 0) {
                    envParams = "${key}='${value}'"
                } else {
                    envParams = envParams + "," + "${key}='${value}'"
                }
            }
            if(envParams.length() > 0)
                 envParams="-env \""+envParams+"\""


                paramArea =paramArea+"--${newBoundary}\r\n" +
                        "Content-Disposition: form-data; name=\"COMMAND\"\r\n" +
                        "Content-Type: application/xml; charset=UTF-8\r\n" +
                        "Content-Transfer-Encoding: 8bit\r\n" +
                        "Accept-Language:de-de\r\n" +
                        "\r\n" +
                        "<AppParam><id>COMMANDTORUN</id><value>${"sleep 60"}</value><type></type></AppParam>\r\n"

                paramArea =paramArea+"--${newBoundary}\r\n" +
                        "Content-Disposition: form-data; name=\"EXTRA_PARAMS\"\r\n" +
                        "Content-Type: application/xml; charset=UTF-8\r\n" +
                        "Content-Transfer-Encoding: 8bit\r\n" +
                        "Accept-Language:de-de\r\n" +
                        "\r\n" +
                        "<AppParam><id>EXTRA_PARAMS</id><value>${"-R 'select[type==any]' "+envParams+((PBSResourceProcessingCommand)convertResourceSet(job.resourceSet)).processingString}</value><type></type></AppParam>\r\n"

            paramArea=paramArea+"--${newBoundary}--\r\n" +
                     "\r\n"
            body = body + paramArea
         }

         // --- File Attachment ---
         /*
         if(job.file.size() != 0) {
             String dataArea="--${boundary}"
             job.parameters.eachWithIndex { key, value, pos ->
                 String dataAreaElement = "--${newBoundary} \nContent-Disposition: form-data; name='f1'  \n" +
                         " Content-Type: application/octet-stream\n" +
                         "Content-Transfer-Encoding: UTF-8\n" +
                         "Content-ID: ${filename} \r\n" +
                         "${file}"
             }
             body = body + dataArea
         }*/

         body = body + "--${boundary}--\r\n"
         logger.info("request body:\n"+body)

         RestResult result = restExecutionService.execute(new RestCommand(resource, body, headers, RestCommand.HttpMethod.HTTPPOST))
         if(result.statusCode == 200){
             logger.info("status code: " + result.statusCode + " result:" + new XmlSlurper().parseText(result.body))
             job.setRunResult(new JobResult(job.lastCommand, new PBSJobDependencyID(job, new XmlSlurper().parseText(result.body).text()), true, job.tool, job.parameters, job.parentJobs))
         }else{
             logger.warning("status code: "+result.statusCode+" result: "+result.body)
         }
     }

     /**
      *
      */
     void abortJob(Job job){
         submitCommand("bkill ${job.getJobID()}")
     }


     void suspendJob(Job job){
         submitCommand("bstop ${job.getJobID()}")
     }

     /**
      *
      * @param jobList
      */
     void resumeJob(Job job){
         submitCommand("bresume ${job.getJobID()}")
     }

     /**
      *
      * @param jobList
      */
     void requeueJob(Job job){
         submitCommand("brequeue ${job.getJobID()}")
     }


     private String prepareURLWithParam(List<Job>jobListParameter){
         String jobIds=""
         jobListParameter.eachWithIndex {job, index ->
             if (index == 0){
                 jobIds= job.getJobID()
             }else{
                 jobIds= jobIds+","+job.getJobID()
             }
         }
         return jobIds
     }
     /**
      * Generic method to submit any LSF valid command e.g. bstop <job id>
      * @param cmd LSF command
      */
     public void submitCommand(String cmd){
         String resource = URI_USER_COMMAND
         List<Header> headers = []
         headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "application/xml "))
         headers.add(new BasicHeader("Accept", "text/plain,application/xml,text/xml,multipart/mixed"))
         String body = "<UserCmd>" +
                 "<cmd>${cmd}</cmd>" +
                 "</UserCmd>"
         def result =restExecutionService.execute(new RestCommand(resource,body,headers,RestCommand.HttpMethod.HTTPPOST))
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
     void getJobdetails(List<Job> jobList){
         String resource = URI_JOB_DETAILS
         List<Header> headers = []
         headers.add(new BasicHeader("Accept", "text/xml,application/xml;"))

         def result =restExecutionService.execute(new RestCommand(resource+prepareURLWithParam(jobList),null,headers,RestCommand.HttpMethod.HTTPGET))
         if(result.statusCode == 200){
             GPathResult res = new XmlSlurper().parseText(result.body)
             logger.info("status code: " + result.statusCode + " result:" + result.body)
             res.job.each {element ->
                 Job job = jobList.find {it.getJobID().equalsIgnoreCase(element.jobId.text()) }
                 GenericJobInfo jobInfo

                 if(job.getJobInfo() != null)
                     jobInfo = job.getJobInfo()
                 else
                     jobInfo = new GenericJobInfo((String)element.jobName, null,(String)element.jobId, null, null)

                 jobInfo.setUser(element.user.text())
                 jobInfo.setCpuTime(element.cpuTime.text())
                 jobInfo.setSystemTime(element.getSystemTime.text())
                 jobInfo.setUserTime(element.getUserTime.text())
                 jobInfo.setStartTimeGMT(element.startTime.text())
                 jobInfo.setSubTimeGMT(element.submitTime.text())
                 jobInfo.setEndTimeGMT(element.endTime.text())
                 jobInfo.setQueue(element.queue.text())
                 jobInfo.setExHosts(element.exHosts.text())
                 jobInfo.setSubHost(element.fromHost.text())
                 jobInfo.setJobGroup(element.jobGroup.text())
                 jobInfo.setSwap(element.swap.text())
                 jobInfo.setDescription(element.description.text())
                 jobInfo.setUserGroup(element.userGroup.text())
                 jobInfo.setMemory(element.mem.text())
                 jobInfo.setRunTime(element.runTime.text())
                 jobInfo.setRunLimit(element.runLimit.text())
                 jobInfo.setNumProcessors(element.numProcessors.text())
                 jobInfo.setNthreads(element.nthreads.text())
                 jobInfo.setProjectName(element.projectName.text())
                 jobInfo.setExitStatus(element.exitStatus.text())
                 jobInfo.setPidStr(element.pidStr.text())
                 jobInfo.setPgidStr(element.pgidStr.text())
                 jobInfo.setCwd(element.cwd.text())
                 jobInfo.setPendReason(element.pendReason.text())
                 jobInfo.setExecCwd(element.execCwd.text())
                 jobInfo.setPriority(element.priority.text())
                 jobInfo.setOutfile(element.outfile.text())
                 jobInfo.setInfile(element.infile.text())
                 jobInfo.setResReq(element.resReq.text())
                 jobInfo.setExecHome(element.execHome.text())
                 jobInfo.setExecUserName(element.execUserName.text())
                 jobInfo.setAskedHostsStr(element.askedHostsStr.text())

                 job.setJobInfo(jobInfo)

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
      *
      * @param jobList
      */
     void getJobhistory(List<Job> jobList){
         String resource = URI_JOB_HISTORY
         List<Header> headers = []
         headers.add(new BasicHeader("Accept", "text/xml,application/xml;"))

         def result =restExecutionService.execute(new RestCommand(resource+"?ids="+prepareURLWithParam(jobList),null,headers,RestCommand.HttpMethod.HTTPGET))
         if(result.statusCode == 200){
             GPathResult res = new XmlSlurper().parseText(result.body)
             logger.info("status code: "+result.statusCode+" result:"+result.body)

             res.history.each {element ->

                 Job job = jobList.find {it.getJobID().equalsIgnoreCase(element.jobSummary.id.text()) }
                 GenericJobInfo jobInfo

                 if(job.getJobInfo() != null)
                     jobInfo = job.getJobInfo()
                 else
                     jobInfo = new GenericJobInfo(element.jobSummary.jobName.text(), null,element.jobSummary.id.text(), null, null)

                 jobInfo.setTimeOfCalculation(element.timeSummary.timeOfCalculation.text())
                 jobInfo.setTimeUserSuspState(element.timeSummary.ususpTime.text())
                 jobInfo.setTimePendState(element.timeSummary.pendTime.text())
                 jobInfo.setTimePendSuspState(element.timeSummary.psuspTime.text())
                 jobInfo.setTimeUnknownState(element.timeSummary.unknownTime.text())
                 jobInfo.setTimeSystemSuspState(element.timeSummary.ssuspTime.text())
                 jobInfo.setRunTime(element.timeSummary.runTime.text())

                 job.setJobInfo(jobInfo)
             }

         }else{
             logger.warning("status code: " + result.statusCode + " result: " + result.body)
         }
     }




}
