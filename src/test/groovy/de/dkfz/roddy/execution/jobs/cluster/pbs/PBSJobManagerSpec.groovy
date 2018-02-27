/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import de.dkfz.roddy.BEException
import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.cluster.ClusterJobManager
import de.dkfz.roddy.execution.jobs.cluster.lsf.LSFJobManager
import groovy.util.slurpersupport.GPathResult
import spock.lang.Specification

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.time.Duration

class PBSJobManagerSpec extends Specification {

    String rawXmlExample = '''
    <Data>
        <Job>
            <Job_Id>15020227.testServer</Job_Id>
            <Job_Name>workflow_test</Job_Name>
            <Job_Owner>testOwner</Job_Owner>
            <job_state>H</job_state>
            <queue>fast</queue>
            <depend>afterok:15624736.testServer</depend>
            <server>testServer</server>
            <Checkpoint>u</Checkpoint>
            <ctime>1499432861</ctime>
            <Error_Path>logging_root_path/clusterLog/2017-07-07/workflow_test</Error_Path>
            <Hold_Types>u</Hold_Types>
            <Join_Path>oe</Join_Path>
            <Keep_Files>n</Keep_Files>
            <Mail_Points>a</Mail_Points>
            <mtime>1499432861</mtime>
            <start_time>1514572025</start_time>
            <Output_Path>logging_root_path/clusterLog/2017-07-07/workflow_test.o15020227</Output_Path>
            <Priority>0</Priority>
            <qtime>1499432861</qtime>
            <Rerunable>True</Rerunable>
            <Walltime><Remaining>71497</Remaining></Walltime>
            <Resource_List>
                <mem>5120mb</mem>
                <nodect>1</nodect>
                <nodes>1:ppn=1</nodes>
                <walltime>00:20:00</walltime>
            </Resource_List>
            <resources_used>
                <cput>04:50:19</cput>
                <energy_used>0</energy_used>
                <mem>2449672kb</mem>
                <vmem>2524268kb</vmem>
                <walltime>03:07:41</walltime>
            </resources_used>
            <Variable_List>PBS_O_QUEUE=default,PBS_O_HOME=/home/test,PBS_O_LOGNAME=test,PBS_O_SHELL=/bin/bash,PBS_O_LANG=en_GB.UTF-8</Variable_List>
            <euser>test</euser>
            <egroup>testGroup</egroup>
            <queue_type>E</queue_type>
            <submit_args>-N workflow_test -h -o /clusterLog/2017-07-07 -j oe -W umask=027 -l mem=5120M -l walltime=00:00:20:00 -l nodes=1:ppn=1</submit_args>
            <umask>23</umask>
            <fault_tolerant>False</fault_tolerant>
            <job_radix>0</job_radix>
            <submit_host>sub.testServer</submit_host>
            <start_count>1</start_count>
            <request_version>1</request_version>
        </Job>
    </Data>
    '''

    void testParseJobDetails() {
        given:
        def parms = JobManagerOptions.create().build()
        TestExecutionService testExecutionService = new TestExecutionService("test", "test")
        PBSJobManager jm = new PBSJobManager(testExecutionService, parms)
        Method method = PBSJobManager.class.getDeclaredMethod("processQstatOutput", List)
        method.setAccessible(true)

        when:
        Map<BEJobID, GenericJobInfo> jobInfo = (Map<BEJobID, GenericJobInfo>) method.invoke(jm, [rawXmlExample])

        then:
        jobInfo.size() == 1
        jobInfo.get(new BEJobID("15020227")).jobName == "workflow_test"
    }



    void "processQstatOutput, replace placeholder PBS_JOBID in logFile and errorLogFile with job id "() {
        given:
        String rawXMLOutput='''
        <Data>
            <Job>
                <Job_Id>15976927.testServer</Job_Id>
                <Output_Path>/logging_root_path/clusterLog/2017-07-07/workflow_test/snvFilter.o$PBS_JOBID</Output_Path>
                <Error_Path>tbi-pbs:/logging_root_path/clusterLog/2017-07-07/workflow_test/snvFilter.e$PBS_JOBID</Error_Path>
            </Job>
        </Data>
    '''

        def parms = JobManagerOptions.create().build()
        TestExecutionService testExecutionService = new TestExecutionService("test", "test")
        PBSJobManager jm = new PBSJobManager(testExecutionService, parms)
        Method method = PBSJobManager.class.getDeclaredMethod("processQstatOutput", List)
        method.setAccessible(true)

        when:
        Map<BEJobID, GenericJobInfo> jobInfo = (Map<BEJobID, GenericJobInfo>) method.invoke(jm, [rawXMLOutput])

        then:
        jobInfo.size() == 1
        jobInfo.get(new BEJobID("15976927")).logFile.toString() == "/logging_root_path/clusterLog/2017-07-07/workflow_test/snvFilter.o15976927.testServer"
        jobInfo.get(new BEJobID("15976927")).errorLogFile.toString() == "/logging_root_path/clusterLog/2017-07-07/workflow_test/snvFilter.e15976927.testServer"
    }


    void testParseDuration() {
        given:
        Method method = ClusterJobManager.class.getDeclaredMethod("parseColonSeparatedHHMMSSDuration", String)
        method.setAccessible(true)

        expect:
        parsedDuration == method.invoke(null, input)

        where:
        input       || parsedDuration
        "00:00:00"  || Duration.ofSeconds(0)
        "24:00:00"  || Duration.ofHours(24)
        "119:00:00" || Duration.ofHours(119)
    }

    void "testParseDuration fails"() {
        given:
        Method method = ClusterJobManager.class.getDeclaredMethod("parseColonSeparatedHHMMSSDuration", String)
        method.setAccessible(true)

        when:
        method.invoke(null, "02:42")

        then:
        InvocationTargetException e = thrown(InvocationTargetException)
        e.targetException.message == "Duration string is not of the format HH+:MM:SS: '02:42'"
    }
}
