/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import groovy.json.JsonSlurper
import spock.lang.Specification

import java.lang.reflect.Method

class LSFJobManagerSpec extends Specification {


    final static String RAW_JSON_OUTPUT = '''
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
      "PIDS":"46782,46796,46798,46915,47458,47643",
      "EXIT_CODE":"",
      "FROM_HOST":"tbi-cn013",
      "EXEC_HOST":"tbi-cn019:tbi-cn019",
      "SUBMIT_TIME":"Dec 28 19:56",
      "START_TIME":"Dec 28 19:56",
      "FINISH_TIME":"Dec 28 19:56 L",
      "CPU_USED":"00:00:01",
      "RUN_TIME":"00:00:01",
      "USER_GROUP":"",
      "SWAP":"",
      "MAX_MEM":"5.2 Gbytes",
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

    final static String RAW_JSON_OUTPUT_WITHOUT_LISTS = '''
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
      "EXIT_CODE":"1",
      "FROM_HOST":"tbi-cn013",
      "EXEC_HOST":"tbi-cn020",
      "SUBMIT_TIME":"Dec 28 19:56",
      "START_TIME":"Dec 28 19:56",
      "FINISH_TIME":"Dec 28 19:56 L",
      "CPU_USED":"00:00:01",
      "RUN_TIME":"00:00:01",
      "USER_GROUP":"",
      "SWAP":"0 Mbytes",
      "MAX_MEM":"522 MBytes",
      "RUNTIMELIMIT":"00:10:00",
      "SUB_CWD":"$HOME",
      "PEND_REASON":"Job dependency condition not satisfied;",
      "EXEC_CWD":"\\/home\\/otptest",
      "OUTPUT_FILE":"\\/sequencing\\/whole_genome_sequencing\\/coveragePlotSingle.o30060",
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

    void "queryJobInfo, bjobs JSON output with lists  "() {

        given:
        def parms = JobManagerOptions.create().build()
        TestExecutionService testExecutionService = new TestExecutionService("test", "test")
        LSFJobManager jm = new LSFJobManager(testExecutionService, parms)
        Method method = LSFJobManager.class.getDeclaredMethod("queryJobInfo", Map)
        method.setAccessible(true)
        Object parsedJson = new JsonSlurper().parseText(RAW_JSON_OUTPUT)
        List records = (List) parsedJson.getAt("RECORDS")

        when:
        GenericJobInfo jobInfo = method.invoke(jm, records.get(0))

        then:
        jobInfo != null
        jobInfo.tool == new File("ls -l")
    }

    void "queryJobInfo, bjobs JSON output without lists  "() {

        given:
        def parms = JobManagerOptions.create().build()
        TestExecutionService testExecutionService = new TestExecutionService("test", "test")
        LSFJobManager jm = new LSFJobManager(testExecutionService, parms)
        Method method = LSFJobManager.class.getDeclaredMethod("queryJobInfo", Map)
        method.setAccessible(true)
        Object parsedJson = new JsonSlurper().parseText(RAW_JSON_OUTPUT_WITHOUT_LISTS)
        List records = (List) parsedJson.getAt("RECORDS")

        when:
        GenericJobInfo jobInfo = method.invoke(jm, records.get(0))

        then:
        jobInfo != null
        jobInfo.tool == new File("ls -l")
    }

    void "queryJobInfo, bjobs JSON output empty  "() {

        given:
        String emptyRawJsonOutput= '''
        {
            "COMMAND":"bjobs",
            "JOBS":1,
            "RECORDS":[
                {
                "JOBID":"22005",
                }
            ]
        }
        '''
        def parms = JobManagerOptions.create().build()
        TestExecutionService testExecutionService = new TestExecutionService("test", "test")
        LSFJobManager jm = new LSFJobManager(testExecutionService, parms)
        Method method = LSFJobManager.class.getDeclaredMethod("queryJobInfo", Map)
        method.setAccessible(true)
        Object parsedJson = new JsonSlurper().parseText(emptyRawJsonOutput)
        List records = (List) parsedJson.getAt("RECORDS")

        when:
        GenericJobInfo jobInfo = method.invoke(jm, records.get(0))

        then:
        jobInfo.id == "22005"
    }

}
