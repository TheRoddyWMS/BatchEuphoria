/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest

import de.dkfz.eilslabs.batcheuphoria.execution.ExecutionService
import de.dkfz.eilslabs.batcheuphoria.jobs.JobManagerCreationParameters

/**
 * REST job manager for LSF cluster systems.
 *
 *
 */
class LSFRestJobManager extends RestJobManager
 {


     /*REST RESOURCES*/
     static {

         URI_JOB_SUBMIT = "/jobs/submit"
         URI_JOB_KILL = "/jobs/kill"
         URI_JOB_SUSPEND = "/jobs/suspend"
         URI_JOB_RESUME = "/jobs/resume"
         URI_JOB_REQUEUE = "/jobs/requeue"
         URI_JOB_DETAILS = "/jobs/"
         URI_JOB_HISTORY = "/jobhistory"
         URI_USER_COMMAND = "/userCmd"
     }

     /*PARAMETERS */
     enum Rest_Resources {

         URI_JOB_SUBMIT ("/jobs/submit"),
         URI_JOB_KILL ("/jobs/kill"),
         URI_JOB_SUSPEND ("/jobs/suspend"),
         URI_JOB_RESUME ("/jobs/resume"),
         URI_JOB_REQUEUE ("/jobs/requeue"),
         URI_JOB_DETAILS ("/jobs/"),
         URI_JOB_HISTORY ("/jobhistory"),
         URI_USER_COMMAND ("/userCmd"),

         final String value

         Rest_Resources(String value) {
             this.value = value
         }

         String getValue() {
             return this.value
         }

         String toString(){
             value
         }

         String getKey() {
             name()
         }
     }

     /*PARAMETERS */
     enum Parameters {


         COMMANDTORUN("COMMANDTORUN"),
         EXTRA_PARAMS ("EXTRA_PARAMS"),
         MAX_MEMORY ("MAX_MEM"),
         MAX_NUMBER_CPU ("MAX_NUM_CPU"),
         MIN_NUMBER_CPU ("MIN_NUM_CPU"),
         PROC_PRE_HOST ("PROC_PRE_HOST"),
         EXTRA_RES ("EXTRA_RES"),
         RUN_LIMIT_MINUTE ("RUNLIMITMINUTE"),
         RERUNABLE ("RERUNABLE"),
         JOB_NAME ("JOB_NAME"),
         APP_PROFILE ("APP_PROFILE"),
         PROJECT_NAME ("PRJ_NAME"),
         RES_ID ("RES_ID"),
         LOGIN_SHELL ("LOGIN_SHELL"),
         QUEUE ("QUEUE"),
         INPUT_FILE ("INPUT_FILE"),
         OUTPUT_FILE ("OUTPUT_FILE"),
         ERROR_FILE ("ERROR_FILE"),

         final String value

         Parameters(String value) {

             this.value = value
         }


         String getValue() {

             return this.value
         }


         String toString(){

             value
         }


         String getKey() {

             name()
         }
 }


     LSFRestJobManager(ExecutionService restExecutionService, JobManagerCreationParameters parms){
        super(restExecutionService, parms)
     }








}
