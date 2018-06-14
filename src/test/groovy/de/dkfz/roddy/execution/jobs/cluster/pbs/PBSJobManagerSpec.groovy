/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.execution.jobs.cluster.ClusterJobManager
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import spock.lang.Specification

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.time.Duration
import java.time.ZoneId
import java.time.ZonedDateTime

class PBSJobManagerSpec extends Specification {
    PBSJobManager manager

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

    String outputQueued = '''<Data><Job><Job_Id>4499334.pbsserver</Job_Id><Job_Name>r180328_183957634_pid_4_starAlignment</Job_Name><Job_Owner>otp-data@subm.example.com</Job_Owner><job_state>Q</job_state><queue>otp</queue><server>pbsserver</server><Checkpoint>u</Checkpoint><ctime>1522255162</ctime><Error_Path>subm:/net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/r180328_183957634_pid_4_starAlignment.o$PBS_JOBID</Error_Path><Hold_Types>n</Hold_Types><Join_Path>oe</Join_Path><Keep_Files>n</Keep_Files><Mail_Points>a</Mail_Points><mtime>1522255162</mtime><Output_Path>subm:/net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/r180328_183957634_pid_4_starAlignment.o$PBS_JOBID</Output_Path><Priority>0</Priority><qtime>1522255162</qtime><Rerunable>True</Rerunable><Resource_List><cput>168:00:00</cput><mem>37888mb</mem><ncpus>1</ncpus><nice>5</nice><nodect>1</nodect><nodes>1:ppn=4</nodes><walltime>05:00:00</walltime></Resource_List><Variable_List>PBS_O_QUEUE=otp,PBS_O_HOME=/home/otp-data,PBS_O_LOGNAME=otp-data,PBS_O_PATH=/usr/local/bin:/usr/bin:/bin:/usr/games,PBS_O_MAIL=/var/mail/otp-data,PBS_O_SHELL=/bin/bash,PBS_O_LANG=en_US.utf8,TOOL_ID=starAlignment,PARAMETER_FILE=/net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/r180328_183957634_pid_4_starAlignment_3.parameters,CONFIG_FILE=/net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/r180328_183957634_pid_4_starAlignment_3.parameters,debugWrapInScript=false,baseEnvironmentScript=/tbi/software/sourceme,PBS_O_WORKDIR=/home/otp-data,PBS_O_HOST=subm.example.com,PBS_O_SERVER=pbsserver</Variable_List><etime>1522255162</etime><submit_args>-v TOOL_ID=starAlignment,PARAMETER_FILE=/net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/r180328_183957634_pid_4_starAlignment_3.parameters,CONFIG_FILE=/net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/r180328_183957634_pid_4_starAlignment_3.parameters,debugWrapInScript=false,baseEnvironmentScript=/tbi/software/sourceme -N r180328_183957634_pid_4_starAlignment -h -w /home/otp-data -j oe -o /net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/r180328_183957634_pid_4_starAlignment.o$PBS_JOBID -l mem=37888M -l walltime=00:05:00:00 -l nodes=1:ppn=4 -q otp /net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/analysisTools/roddyTools/wrapInScript.sh</submit_args><fault_tolerant>False</fault_tolerant><job_radix>0</job_radix><submit_host>subm.example.com</submit_host><init_work_dir>/home/otp-data</init_work_dir></Job></Data>

'''

    String outputFinished = '''<Data><Job><Job_Id>4564045.pbsserver</Job_Id><Job_Name>r180405_163953553_stds_snvJoinVcfFiles</Job_Name><Job_Owner>otp-data@subm.example.com</Job_Owner><resources_used><cput>00:00:00</cput><mem>6064kb</mem><vmem>454232kb</vmem><walltime>00:00:06</walltime></resources_used><job_state>C</job_state><queue>otp</queue><server>pbsserver</server><Checkpoint>u</Checkpoint><ctime>1522939158</ctime><depend>afterok:6059780.pbsserver@pbsserver:6059781.pbsserver@pbsserver,beforeok:6059788.pbsserver@pbsserver</depend><Error_Path>subm:/net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles.o$PBS_JOBID</Error_Path><exec_host>denbi5-int/0+denbi5-int/1+denbi5-int/2</exec_host><exec_port>15003</exec_port><Hold_Types>n</Hold_Types><Join_Path>oe</Join_Path><Keep_Files>n</Keep_Files><Mail_Points>a</Mail_Points><mtime>1522939194</mtime><Output_Path>subm:/net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles.o$PBS_JOBID</Output_Path><Priority>0</Priority><qtime>1522939158</qtime><Rerunable>True</Rerunable><Resource_List><cput>168:00:00</cput><mem>1024mb</mem><ncpus>1</ncpus><nice>5</nice><nodect>1</nodect><nodes>1:ppn=3</nodes><walltime>04:00:00</walltime></Resource_List><session_id>14506</session_id><Variable_List>PBS_O_QUEUE=otp,PBS_O_HOME=/home/otp-data,PBS_O_LOGNAME=otp-data,PBS_O_PATH=/usr/local/bin:/usr/bin:/bin:/usr/games,PBS_O_MAIL=/var/mail/otp-data,PBS_O_SHELL=/bin/bash,PBS_O_LANG=en_US.utf8,TOOL_ID=snvJoinVcfFiles,PARAMETER_FILE=/net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles_9.parameters,CONFIG_FILE=/net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles_9.parameters,debugWrapInScript=false,baseEnvironmentScript=/tbi/software/sourceme,PBS_O_WORKDIR=/home/otp-data,PBS_O_HOST=subm.example.com,PBS_O_SERVER=pbsserver</Variable_List><sched_hint>Post job file processing error; job 4564045.pbsserver on host denbi5-int/1

Unable to copy file /var/spool/torque/spool/4564045.pbsserver.OU to otp-data@subm:/net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles.o4564045.pbsserver
*** error from copy
Host key verification failed.
lost connection
*** end error output
Output retained on that host in: /var/spool/torque/undelivered/4564045.pbsserver.OU</sched_hint><etime>1522939182</etime><exit_status>0</exit_status><submit_args>-v TOOL_ID=snvJoinVcfFiles,PARAMETER_FILE=/net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles_9.parameters,CONFIG_FILE=/net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles_9.parameters,debugWrapInScript=false,baseEnvironmentScript=/tbi/software/sourceme -N r180405_163953553_stds_snvJoinVcfFiles -h -w /home/otp-data -j oe -o /net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles.o$PBS_JOBID -l mem=1024M -l walltime=00:04:00:00 -l nodes=1:ppn=3 -q otp -W depend=afterok:6059780:6059781 /net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/analysisTools/roddyTools/wrapInScript.sh</submit_args><start_time>1522939183</start_time><start_count>1</start_count><fault_tolerant>False</fault_tolerant><comp_time>1522939194</comp_time><job_radix>0</job_radix><total_runtime>10.431524</total_runtime><submit_host>subm.example.com</submit_host><init_work_dir>/home/otp-data</init_work_dir></Job></Data>
'''

    void setup() {
        JobManagerOptions parms = JobManagerOptions.create().build()
        TestExecutionService testExecutionService = new TestExecutionService("test", "test")
        manager = new PBSJobManager(testExecutionService, parms)
    }

    void "test processQstatOutputFromXML with queued job"() {
        when:
        Map<BEJobID, GenericJobInfo> result = manager.processQstatOutputFromXML(outputQueued)

        then:
        result.size() == 1
        GenericJobInfo jobInfo = result.get(new BEJobID("4499334"))
        jobInfo
        jobInfo.askedResources.size == null
        jobInfo.askedResources.mem == new BufferValue(37888, BufferUnit.M)
        jobInfo.askedResources.cores == 4
        jobInfo.askedResources.nodes == 1
        jobInfo.askedResources.walltime == Duration.ofHours(5)
        jobInfo.askedResources.storage == null
        jobInfo.askedResources.queue == "otp"
        jobInfo.askedResources.nthreads == null
        jobInfo.askedResources.swap == null

        jobInfo.usedResources.size == null
        jobInfo.usedResources.mem == null
        jobInfo.usedResources.cores == null
        jobInfo.usedResources.nodes == null
        jobInfo.usedResources.walltime == null
        jobInfo.usedResources.storage == null
        jobInfo.usedResources.queue == "otp"
        jobInfo.usedResources.nthreads == null
        jobInfo.usedResources.swap == null

        jobInfo.jobName == "r180328_183957634_pid_4_starAlignment"
        jobInfo.tool == null
        jobInfo.jobID == new BEJobID("4499334")
        jobInfo.submitTime == ZonedDateTime.of(2018, 03, 28, 18, 39, 22, 0, ZoneId.systemDefault())
        jobInfo.eligibleTime == ZonedDateTime.of(2018, 03, 28, 18, 39, 22, 0, ZoneId.systemDefault())
        jobInfo.startTime == null
        jobInfo.endTime == null
        jobInfo.executionHosts == null
        jobInfo.submissionHost == "subm.example.com"
        jobInfo.priority == "0"
        jobInfo.logFile == new File("/net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/r180328_183957634_pid_4_starAlignment.o4499334.pbsserver")
        jobInfo.errorLogFile == new File("/net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/r180328_183957634_pid_4_starAlignment.o4499334.pbsserver")
        jobInfo.inputFile == null
        jobInfo.user == null
        jobInfo.userGroup == null
        jobInfo.resourceReq == '-v TOOL_ID=starAlignment,PARAMETER_FILE=/net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/r180328_183957634_pid_4_starAlignment_3.parameters,CONFIG_FILE=/net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/r180328_183957634_pid_4_starAlignment_3.parameters,debugWrapInScript=false,baseEnvironmentScript=/tbi/software/sourceme -N r180328_183957634_pid_4_starAlignment -h -w /home/otp-data -j oe -o /net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/r180328_183957634_pid_4_starAlignment.o$PBS_JOBID -l mem=37888M -l walltime=00:05:00:00 -l nodes=1:ppn=4 -q otp /net/isilon/otp/workflow-tests/tmp/RnaPairedAlignmentWorkflow-otp-web-2018-03-28-18-36-57-153+0200-mFrFsCEXnGMdFEGC/root_path/projectDirName_20/sequencing/rna_sequencing/view-by-pid/pid_4/control/paired/merged-alignment/.merging_0/roddyExecutionStore/exec_180328_183957634_otp-data_RNA/analysisTools/roddyTools/wrapInScript.sh'
        jobInfo.startCount == null
        jobInfo.account == null
        jobInfo.server == "pbsserver"
        jobInfo.umask == null
        jobInfo.parameters == null
        jobInfo.parentJobIDs == null
        jobInfo.otherSettings == null
        jobInfo.jobState == JobState.QUEUED
        jobInfo.userTime == null
        jobInfo.systemTime == null
        jobInfo.pendReason == null
        jobInfo.execHome == null
        jobInfo.execUserName == null
        jobInfo.pidStr == null
        jobInfo.pgidStr == null
        jobInfo.exitCode == null
        jobInfo.jobGroup == null
        jobInfo.description == null
        jobInfo.execCwd == null //???
        jobInfo.askedHostsStr == null
        jobInfo.cwd == null
        jobInfo.projectName == null
        jobInfo.cpuTime == null
        jobInfo.runTime == null
        jobInfo.timeUserSuspState == null
        jobInfo.timePendState == null
        jobInfo.timePendSuspState == null
        jobInfo.timeSystemSuspState == null
        jobInfo.timeUnknownState == null
        jobInfo.timeOfCalculation == null
    }

    void "test processQstatOutputFromXML with finished job, with newline"() {
        when:
        Map<BEJobID, GenericJobInfo> result = manager.processQstatOutputFromXML(outputFinished)

        then:
        result.size() == 1
        GenericJobInfo jobInfo = result.get(new BEJobID("4564045"))
        jobInfo
        jobInfo.askedResources.size == null
        jobInfo.askedResources.mem == new BufferValue(1024, BufferUnit.M)
        jobInfo.askedResources.cores == 3
        jobInfo.askedResources.nodes == 1
        jobInfo.askedResources.walltime == Duration.ofHours(4)
        jobInfo.askedResources.storage == null
        jobInfo.askedResources.queue == "otp"
        jobInfo.askedResources.nthreads == null
        jobInfo.askedResources.swap == null

        jobInfo.usedResources.size == null
        jobInfo.usedResources.mem == new BufferValue(6064, BufferUnit.k)
        jobInfo.usedResources.cores == null
        jobInfo.usedResources.nodes == null
        jobInfo.usedResources.walltime == Duration.ofSeconds(6)
        jobInfo.usedResources.storage == null
        jobInfo.usedResources.queue == "otp"
        jobInfo.usedResources.nthreads == null
        jobInfo.usedResources.swap == null

        jobInfo.jobName == "r180405_163953553_stds_snvJoinVcfFiles"
        jobInfo.tool == null
        jobInfo.jobID == new BEJobID("4564045")
        jobInfo.submitTime == ZonedDateTime.of(2018, 4, 5, 16, 39, 18, 0, ZoneId.systemDefault())
        jobInfo.eligibleTime == ZonedDateTime.of(2018, 4, 5, 16, 39, 42, 0, ZoneId.systemDefault())
        jobInfo.startTime == ZonedDateTime.of(2018, 4, 5, 16, 39, 43, 0, ZoneId.systemDefault())
        jobInfo.endTime == ZonedDateTime.of(2018, 4, 5, 16, 39, 54, 0, ZoneId.systemDefault())
        jobInfo.executionHosts == ["denbi5-int"]
        jobInfo.submissionHost == "subm.example.com"
        jobInfo.priority == "0"
        jobInfo.logFile == new File("/net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles.o4564045.pbsserver")
        jobInfo.errorLogFile == new File("/net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles.o4564045.pbsserver")
        jobInfo.inputFile == null
        jobInfo.user == null
        jobInfo.userGroup == null
        jobInfo.resourceReq == '-v TOOL_ID=snvJoinVcfFiles,PARAMETER_FILE=/net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles_9.parameters,CONFIG_FILE=/net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles_9.parameters,debugWrapInScript=false,baseEnvironmentScript=/tbi/software/sourceme -N r180405_163953553_stds_snvJoinVcfFiles -h -w /home/otp-data -j oe -o /net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/r180405_163953553_stds_snvJoinVcfFiles.o$PBS_JOBID -l mem=1024M -l walltime=00:04:00:00 -l nodes=1:ppn=3 -q otp -W depend=afterok:6059780:6059781 /net/isilon/otp/workflow-tests/tmp/RoddyBamFileWgsRoddySnvWorkflow-otp-web-2018-04-05-16-38-05-094+0200-k4FXdCscdyo3vAxl/root_path/projectDirName_22/sequencing/whole_genome_sequencing/view-by-pid/stds/snv_results/paired/sampletypename-24_sampletypename-43/results_SNVCallingWorkflow-1.2.166-1_v1_0_2018-04-05_16h39_+0200/roddyExecutionStore/exec_180405_163953553_otp-data_WGS/analysisTools/roddyTools/wrapInScript.sh'
        jobInfo.startCount == 1
        jobInfo.account == null
        jobInfo.server == "pbsserver"
        jobInfo.umask == null
        jobInfo.parameters == null
        jobInfo.parentJobIDs == ["6059780", "6059781"]
        jobInfo.otherSettings == null
        jobInfo.jobState == JobState.COMPLETED_UNKNOWN
        jobInfo.userTime == null
        jobInfo.systemTime == null
        jobInfo.pendReason == null
        jobInfo.execHome == null
        jobInfo.execUserName == null
        jobInfo.pidStr == null
        jobInfo.pgidStr == null
        jobInfo.exitCode == 0
        jobInfo.jobGroup == null
        jobInfo.description == null
        jobInfo.execCwd == null
        jobInfo.askedHostsStr == null
        jobInfo.cwd == null
        jobInfo.projectName == null
        jobInfo.cpuTime == Duration.ZERO
        jobInfo.runTime == Duration.ofSeconds(10)
        jobInfo.timeUserSuspState == null
        jobInfo.timePendState == null
        jobInfo.timePendSuspState == null
        jobInfo.timeSystemSuspState == null
        jobInfo.timeUnknownState == null
        jobInfo.timeOfCalculation == null
    }

        void "processQstatOutput, qstat with empty XML output"() {
        given:
        String rawXMLOutput ='''
        <Data>
            <Job>
                <Job_Id>15020227.testServer</Job_Id>
            </Job>
        </Data>
        '''

        when:
        Map<BEJobID, GenericJobInfo> jobInfo = manager.processQstatOutputFromXML(rawXMLOutput)

        then:
        jobInfo.size() == 1
    }

    void "processQstatOutput, qstat with XML output"() {
        when:
        Map<BEJobID, GenericJobInfo> jobInfo = manager.processQstatOutputFromXML(rawXmlExample)

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

        when:
        Map<BEJobID, GenericJobInfo> jobInfo = manager.processQstatOutputFromXML(rawXMLOutput)

        then:
        jobInfo.size() == 1
        jobInfo.get(new BEJobID("15976927")).logFile.toString() == "/logging_root_path/clusterLog/2017-07-07/workflow_test/snvFilter.o15976927.testServer"
        jobInfo.get(new BEJobID("15976927")).errorLogFile.toString() == "/logging_root_path/clusterLog/2017-07-07/workflow_test/snvFilter.e15976927.testServer"
    }


    void "parseColonSeparatedHHMMSSDuration, parse duration"() {

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

  
    void "parseColonSeparatedHHMMSSDuration, parse duration fails"() {

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
