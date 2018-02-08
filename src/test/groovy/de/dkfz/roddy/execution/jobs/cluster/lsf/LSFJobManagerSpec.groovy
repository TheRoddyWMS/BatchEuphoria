/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import de.dkfz.roddy.BEException
import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.cluster.ClusterJobManager
import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSJobManager
import de.dkfz.roddy.tools.LoggerWrapper
import groovy.json.JsonSlurper
import spock.lang.Specification

import java.lang.reflect.Method
import java.time.Duration

class LSFJobManagerSpec extends Specification {

    final static String results = '''
{
    "COMMAND":"bjobs",
    "JOBS":2,
    "RECORDS":[
        {
            "PEND_REASON":"",
            "EXEC_CWD":"\\/home\\/otptest",
            "OUTPUT_FILE":"test.out",
            "INPUT_FILE":"",
            "EFFECTIVE_RESREQ":"select[type == local] order[r15s:pg] ",
            "EXEC_HOME":"\\/home\\/otptest",
            "SLOTS":"1",
            "ERROR_FILE":"",
            "COMMAND":"ls -l",
            "DEPENDENCY":"",
            "SUBMIT_TIME":"Dec 20 16:25",
            "FINISH_TIME":"Dec 20 16:25 L",
            "CPU_USED":"00:00:00",
            "RUN_TIME":"00:00:01",
            "USER_GROUP":"",
            "SWAP":"",
            "MAX_MEM":"",
            "RUNTIMELIMIT":"00:10:00",
            "SUB_CWD":"$HOME",
            "START_TIME":"Dec 20 16:25",
            "JOBID":"21680",
            "JOB_NAME":"ls -l",
            "STAT":"DONE",
            "USER":"otptest",
            "QUEUE":"short-dmg",
            "RU_STIME":"00:00:00",
            "TIME_LEFT":"00:09:59 L"
        },
        {
            "PEND_REASON":"",
            "EXEC_CWD":"\\/home\\/otptest",
            "OUTPUT_FILE":"test.out",
            "INPUT_FILE":"",
            "EFFECTIVE_RESREQ":"select[type == local] order[r15s:pg] ",
            "EXEC_HOME":"\\/home\\/otptest",
            "SLOTS":"1",
            "ERROR_FILE":"",
            "COMMAND":"ls -l",
            "DEPENDENCY":"done(21680)",
            "SUBMIT_TIME":"Dec 20 16:26",
            "FINISH_TIME":"Dec 20 16:26 L",
            "CPU_USED":"00:00:00",
            "RUN_TIME":"00:00:11",
            "USER_GROUP":"",
            "SWAP":"",
            "MAX_MEM":"",
            "RUNTIMELIMIT":"00:10:00",
            "SUB_CWD":"$HOME",
            "START_TIME":"Dec 20 16:26",
            "JOBID":"21681",
            "JOB_NAME":"ls -l",
            "STAT":"DONE",
            "USER":"otptest",
            "QUEUE":"short-dmg",
            "RU_STIME":"00:00:00",
            "TIME_LEFT":"00:09:49 L"
        }
]
}
'''

    final static String result = '''
{
  "COMMAND":"bjobs",
  "JOBS":1,
  "RECORDS":[
    {
      "JOBID":"22005",
      "JOB_NAME":"ls -l",
      "STAT":"DONE",
      "USER":"otptest",
      "QUEUE":"short-dmg",
      "JOB_DESCRIPTION":"",
      "PROJ_NAME":"default",
      "JOB_GROUP":"",
      "JOB_PRIORITY":"",
      "PIDS":"51904",
      "EXIT_CODE":"",
      "FROM_HOST":"tbi-cn013",
      "EXEC_HOST":"tbi-cn020",
      "SUBMIT_TIME":"Dec 28 19:56",
      "START_TIME":"Dec 28 19:56",
      "FINISH_TIME":"Dec 28 19:56 L",
      "CPU_USED":"00:00:01",
      "RUN_TIME":"00:00:01",
      "USER_GROUP":"",
      "SWAP":"",
      "MAX_MEM":"",
      "RUNTIMELIMIT":"00:10:00",
      "SUB_CWD":"$HOME",
      "PEND_REASON":"",
      "EXEC_CWD":"\\/home\\/otptest",
      "OUTPUT_FILE":"",
      "INPUT_FILE":"",
      "EFFECTIVE_RESREQ":"select[type == local] order[r15s:pg] ",
      "EXEC_HOME":"\\/home\\/otptest",
      "SLOTS":"1",
      "ERROR_FILE":"",
      "COMMAND":"ls -l",
      "DEPENDENCY":"done(22004)"
    }
  ]
}
'''


    void testQueryJobInfo() {

        given:
        def parms = JobManagerOptions.create().build()
        TestExecutionService testExecutionService = new TestExecutionService("test","test")
        LSFJobManager jm = new LSFJobManager(testExecutionService,parms)
        Method method = LSFJobManager.class.getDeclaredMethod("queryJobInfo", java.lang.Object)
        method.setAccessible(true)
        Object parsedJson = new JsonSlurper().parseText(result)
        List records = (List) parsedJson.getAt("RECORDS")

        when:
        GenericJobInfo jobInfo = method.invoke(jm, records.get(0))

        then:
        jobInfo != null
        jobInfo.tool == new File("ls -l")
        println jobInfo.toString()


    }

}
