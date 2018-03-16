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
import groovy.transform.CompileStatic
import org.junit.Test
import java.lang.reflect.Method

/**
 * Created by heinold on 26.03.17.
 */
@CompileStatic
class PBSQstatReaderTest {

    final static String output1 = """\
Job Id: 14973441.tbi-pbs-ng.inet.dkfz-heidelberg.de
    Job_Name = r170623_063520503_A056-XQH5TD_createControlBafPlots
    Job_Owner = otproddy@tbi-pbs4.inet.dkfz-heidelberg.de
    resources_used.cput = 00:02:38
    resources_used.energy_used = 0
    resources_used.mem = 472772kb
    resources_used.vmem = 756656kb
    resources_used.walltime = 00:03:44
    job_state = C
    queue = medium
    server = tbi-pbs-ng.inet.dkfz-heidelberg.de
    Checkpoint = u
    ctime = Fri Jun 23 06:35:59 2017
    Error_Path = tbi-pbs4:/icgc/dkfzlsdf/project/hipo/hipo_A056/sequencing/who
\tle_genome_sequencing/view-by-pid/A056-XQH5TD/cnv_results/paired/metast
\tasis_blood/results_ACEseqWorkflow-1.2.8-1_v1_0_2017-06-23_06h33_+0200/
\troddyExecutionStore/exec_170623_063520503_otproddy_WGS/r170623_0635205
\t03_A056-XQH5TD_createControlBafPlots.e14973441
    exec_host = tbi-dsx50/22
    group_list = B080
    Hold_Types = n
    Join_Path = oe
    Keep_Files = n
    Mail_Points = a
    mtime = Fri Jun 23 10:59:30 2017
    Output_Path = tbi-pbs4:/icgc/dkfzlsdf/project/hipo/hipo_A056/sequencing/wh
\tole_genome_sequencing/view-by-pid/A056-XQH5TD/cnv_results/paired/metas
\ttasis_blood/results_ACEseqWorkflow-1.2.8-1_v1_0_2017-06-23_06h33_+0200
\t/roddyExecutionStore/exec_170623_063520503_otproddy_WGS/r170623_063520
\t503_A056-XQH5TD_createControlBafPlots.o14973441
    Priority = 0
    qtime = Sat Jun  3 06:35:59 2017
    Rerunable = True
    Resource_List.mem = 5120mb
    Resource_List.walltime = 01:00:00
    session_id = 34432
    euser = otproddy
    egroup = B080
    queue_type = E
    etime = Fri Jun 23 10:54:49 2017
    exit_status = 0
    submit_args = -N r170623_063520503_A056-XQH5TD_createControlBafPlots -o /i
\tcgc/dkfzlsdf/project/hipo/hipo_A056/sequencing/whole_genome_sequencing
\t/view-by-pid/A056-XQH5TD/cnv_results/paired/metastasis_blood/results_A
\tCEseqWorkflow-1.2.8-1_v1_0_2017-06-23_06h33_+0200/roddyExecutionStore/
\texec_170623_063520503_otproddy_WGS -j oe -W group_list=B080 -W umask=0
\t07 -l mem=5120M -l walltime=00:01:00:00 -W depend=afterok:14973440.tbi
\t-pbs-ng.inet.dkfz-heidelberg.de:14973412.tbi-pbs-ng.inet.dkfz-heidelbe
\trg.de -v WRAPPED_SCRIPT=/icgc/dkfzlsdf/project/hipo/hipo_A056/sequenci
\tng/whole_genome_sequencing/view-by-pid/A056-XQH5TD/cnv_results/paired/
\tmetastasis_blood/results_ACEseqWorkflow-1.2.8-1_v1_0_2017-06-23_06h33_
\t+0200/roddyExecutionStore/exec_170623_063520503_otproddy_WGS/analysisT
\tools/copyNumberEstimationWorkflow/createControlBafPlots.sh,
\tPARAMETER_FILE=/icgc/dkfzlsdf/project/hipo/hipo_A056/sequencing/whole
\t_genome_sequencing/view-by-pid/A056-XQH5TD/cnv_results/paired/metastas
\tis_blood/results_ACEseqWorkflow-1.2.8-1_v1_0_2017-06-23_06h33_+0200/ro
\tddyExecutionStore/exec_170623_063520503_otproddy_WGS/r170623_063520503
\t_A056-XQH5TD_createControlBafPlots_120.parameters /icgc/dkfzlsdf/proje
\tct/hipo/hipo_A056/sequencing/whole_genome_sequencing/view-by-pid/A056-
\tXQH5TD/cnv_results/paired/metastasis_blood/results_ACEseqWorkflow-1.2.
\t8-1_v1_0_2017-06-23_06h33_+0200/roddyExecutionStore/exec_170623_063520
\t503_otproddy_WGS/analysisTools/roddyTools/wrapInScript.sh
    umask = 7
    start_time = Fri Jun 23 10:55:46 2017
    start_count = 1
    fault_tolerant = False
    comp_time = Fri Jun 23 10:59:30 2017
    job_radix = 0
    total_runtime = 279.826458
    submit_host = tbi-pbs4.inet.dkfz-heidelberg.de
    request_version = 1

Job Id: 14973792.tbi-pbs-ng.inet.dkfz-heidelberg.de
    Job_Name = yapima
    Job_Owner = pastor@tbi-worker.inet.dkfz-heidelberg.de
    job_state = Q
    queue = long
    server = tbi-pbs-ng.inet.dkfz-heidelberg.de
    Checkpoint = u
    ctime = Fri Jun 23 11:02:42 2017
    Error_Path = tbi-worker:/ibios/co02/xavier/eo/yapima/yapima.e14973792
    Hold_Types = n
    Join_Path = oe
    Keep_Files = n
    Mail_Points = a
    Mail_Users = output2.pastorhostench@dkfz-heidelberg.de
    mtime = Fri Jun 23 11:02:42 2017
    Output_Path = tbi-worker:/ibios/co02/xavier/eo/yapima/yapima.o14973792
    Priority = 0
    qtime = Fri Jun 23 11:02:42 2017
    Rerunable = True
    Resource_List.mem = 10gb
    Resource_List.nodect = 1
    Resource_List.nodes = 1:ppn=1
    Resource_List.walltime = 03:00:00
    euser = pastor
    egroup = B080
    queue_type = E
    etime = Fri Jun 23 11:02:42 2017
    submit_args = -o /ibios/co02/xavier/eo/yapima -j oe -M output2.pastorhostench@dk
\tfz-heidelberg.de -N yapima -l walltime=3:00:00,nodes=1:ppn=1,
\tmem=10g -v CONFIG_FILE=/icgc/dkfzlsdf/analysis/hipo/hipo_043/methylat
\tion_data_H043/config_yapima.sh /home/pastor/pipelines/run/yapima/proce
\tss450k.sh
    fault_tolerant = False
    job_radix = 0
    submit_host = tbi-worker.inet.dkfz-heidelberg.de
    request_version = 1

Job Id: 14973766.tbi-pbs-ng.inet.dkfz-heidelberg.de
    Job_Name = r170623_105203487_A017-WQL2_snvCalling
    Job_Owner = otproddy@tbi-pbs4.inet.dkfz-heidelberg.de
    resources_used.cput = 00:10:06
    resources_used.energy_used = 0
    resources_used.mem = 146892kb
    resources_used.vmem = 261756kb
    resources_used.walltime = 00:10:03
    job_state = R
    queue = verylong
    server = tbi-pbs-ng.inet.dkfz-heidelberg.de
    Checkpoint = u
    ctime = Fri Jun 23 10:52:20 2017
    depend = beforeok:14973775.tbi-pbs-ng.inet.dkfz-heidelberg.de
    Error_Path = tbi-pbs4:/icgc/dkfzlsdf/project/hipo/hipo_A017/sequencing/who
\tle_genome_sequencing/view-by-pid/A017-WQL2/snv_results/paired/tumor_co
\tntrol02/results_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_10h50_+02
\t00/roddyExecutionStore/exec_170623_105203487_otproddy_WGS/r170623_1052
\t03487_A017-WQL2_snvCalling.e14973766
    exec_host = tbi-dsx55/25
    group_list = B080
    Hold_Types = n
    Join_Path = oe
    Keep_Files = n
    Mail_Points = a
    mtime = Fri Jun 23 10:53:47 2017
    Output_Path = tbi-pbs4:/icgc/dkfzlsdf/project/hipo/hipo_A017/sequencing/wh
\tole_genome_sequencing/view-by-pid/A017-WQL2/snv_results/paired/tumor_c
\tontrol02/results_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_10h50_+0
\t200/roddyExecutionStore/exec_170623_105203487_otproddy_WGS/r170623_105
\t203487_A017-WQL2_snvCalling.o14973766
    Priority = 0
    qtime = Fri Jun 23 10:52:20 2017
    Rerunable = True
    Resource_List.mem = 4096mb
    Resource_List.nodect = 1
    Resource_List.nodes = 1:ppn=1
    Resource_List.walltime = 48:00:00
    session_id = 45434
    euser = otproddy
    egroup = B080
    queue_type = E
    etime = Fri Jun 23 10:52:20 2017
    submit_args = -N r170623_105203487_A017-WQL2_snvCalling -o /icgc/dkfzlsdf/
\tproject/hipo/hipo_A017/sequencing/whole_genome_sequencing/view-by-pid/
\tA017-WQL2/snv_results/paired/tumor_control02/results_SNVCallingWorkflo
\tw-1.0.166-1_v1_0_2017-06-23_10h50_+0200/roddyExecutionStore/exec_17062
\t3_105203487_otproddy_WGS -j oe -W group_list=B080 -W umask=007 -l mem=
\t4096m -l nodes=1:ppn=1 -l walltime=48:00:00 -v WRAPPED_SCRIPT=/icgc/dk
\tfzlsdf/project/hipo/hipo_A017/sequencing/whole_genome_sequencing/view-
\tby-pid/A017-WQL2/snv_results/paired/tumor_control02/results_SNVCalling
\tWorkflow-1.0.166-1_v1_0_2017-06-23_10h50_+0200/roddyExecutionStore/exe
\tc_170623_105203487_otproddy_WGS/analysisTools/snvPipeline/snvCalling.output1
\th,
\tPARAMETER_FILE=/icgc/dkfzlsdf/project/hipo/hipo_A017/sequencing/whole
\t_genome_sequencing/view-by-pid/A017-WQL2/snv_results/paired/tumor_cont
\trol02/results_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_10h50_+0200
\t/roddyExecutionStore/exec_170623_105203487_otproddy_WGS/r170623_105203
\t487_A017-WQL2_snvCalling_50.parameters /icgc/dkfzlsdf/project/hipo/hip
\to_A017/sequencing/whole_genome_sequencing/view-by-pid/A017-WQL2/snv_re
\tsults/paired/tumor_control02/results_SNVCallingWorkflow-1.0.166-1_v1_0
\t_2017-06-23_10h50_+0200/roddyExecutionStore/exec_170623_105203487_otpr
\toddy_WGS/analysisTools/roddyTools/wrapInScript.sh
    umask = 7
    start_time = Fri Jun 23 10:53:47 2017
    Walltime.Remaining = 172137
    start_count = 1
    fault_tolerant = False
    job_radix = 0
    submit_host = tbi-pbs4.inet.dkfz-heidelberg.de
    request_version = 1

Job Id: 14973745.tbi-pbs-ng.inet.dkfz-heidelberg.de
    Job_Name = r170623_10495311_A017-39867J_snvAnnotation
    Job_Owner = otproddy@tbi-pbs4.inet.dkfz-heidelberg.de
    job_state = H
    queue = verylong
    server = tbi-pbs-ng.inet.dkfz-heidelberg.de
    Checkpoint = u
    ctime = Fri Jun 23 10:50:14 2017
    depend = afterok:14973744.tbi-pbs-ng.inet.dkfz-heidelberg.de,
\tbeforeok:14973746.tbi-pbs-ng.inet.dkfz-heidelberg.de
    Error_Path = tbi-pbs4:/icgc/dkfzlsdf/project/hipo/hipo_A017/sequencing/who
\tle_genome_sequencing/view-by-pid/A017-39867J/snv_results/paired/metast
\tasis_control02/results_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_10
\th48_+0200/roddyExecutionStore/exec_170623_10495311_otproddy_WGS/r17062
\t3_10495311_A017-39867J_snvAnnotation.e14973745
    group_list = B080
    Hold_Types = output1
    Join_Path = oe
    Keep_Files = n
    Mail_Points = a
    mtime = Fri Jun 23 10:50:15 2017
    Output_Path = tbi-pbs4:/icgc/dkfzlsdf/project/hipo/hipo_A017/sequencing/wh
\tole_genome_sequencing/view-by-pid/A017-39867J/snv_results/paired/metas
\ttasis_control02/results_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_1
\t0h48_+0200/roddyExecutionStore/exec_170623_10495311_otproddy_WGS/r1706
\t23_10495311_A017-39867J_snvAnnotation.o14973745
    Priority = 0
    qtime = Fri Jun 23 10:50:14 2017
    Rerunable = True
    Resource_List.mem = 4096mb
    Resource_List.nodect = 1
    Resource_List.nodes = 1:ppn=2
    Resource_List.walltime = 180:00:00
    euser = otproddy
    egroup = B080
    queue_type = E
    submit_args = -N r170623_10495311_A017-39867J_snvAnnotation -o /icgc/dkfzl
\tsdf/project/hipo/hipo_A017/sequencing/whole_genome_sequencing/view-by-
\tpid/A017-39867J/snv_results/paired/metastasis_control02/results_SNVCal
\tlingWorkflow-1.0.166-1_v1_0_2017-06-23_10h48_+0200/roddyExecutionStore
\t/exec_170623_10495311_otproddy_WGS -j oe -W group_list=B080 -W umask=0
\t07 -l mem=4096m -l nodes=1:ppn=2 -l walltime=180:00:00 -W depend=after
\tok:14973744.tbi-pbs-ng.inet.dkfz-heidelberg.de -v WRAPPED_SCRIPT=/icgc
\t/dkfzlsdf/project/hipo/hipo_A017/sequencing/whole_genome_sequencing/vi
\tew-by-pid/A017-39867J/snv_results/paired/metastasis_control02/results_
\tSNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_10h48_+0200/roddyExecutio
\tnStore/exec_170623_10495311_otproddy_WGS/analysisTools/snvPipeline/snv
\tAnnotation.sh,
\tPARAMETER_FILE=/icgc/dkfzlsdf/project/hipo/hipo_A017/sequencing/whole
\t_genome_sequencing/view-by-pid/A017-39867J/snv_results/paired/metastas
\tis_control02/results_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_10h4
\t8_+0200/roddyExecutionStore/exec_170623_10495311_otproddy_WGS/r170623_
\t10495311_A017-39867J_snvAnnotation_60.parameters /icgc/dkfzlsdf/projec
\tt/hipo/hipo_A017/sequencing/whole_genome_sequencing/view-by-pid/A017-3
\t9867J/snv_results/paired/metastasis_control02/results_SNVCallingWorkfl
\tow-1.0.166-1_v1_0_2017-06-23_10h48_+0200/roddyExecutionStore/exec_1706
\t23_10495311_otproddy_WGS/analysisTools/roddyTools/wrapInScript.sh
    umask = 7
    fault_tolerant = False
    job_radix = 0
    submit_host = tbi-pbs4.inet.dkfz-heidelberg.de
    request_version = 1

"""


    final static String output2 = """
Job Id: 14973826.tbi-pbs-ng.inet.dkfz-heidelberg.de
    Job_Name = r170623_111000536_XI061_9EP29_snvDeepAnnotation
    Job_Owner = otproddy@tbi-pbs4.inet.dkfz-heidelberg.de
    job_state = H
    queue = long
    server = tbi-pbs-ng.inet.dkfz-heidelberg.de
    Checkpoint = u
    ctime = Fri Jun 23 11:10:25 2017
    depend = afterok:14973825.tbi-pbs-ng.inet.dkfz-heidelberg.de,
	beforeok:14973827.tbi-pbs-ng.inet.dkfz-heidelberg.de
    Error_Path = tbi-pbs4:/icgc/dkfzlsdf/project/Xintern/XI061_ependymoma/sequ
	encing/whole_genome_sequencing/view-by-pid/XI061_9EP29/snv_results/pai
	red/tumor_blood/results_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_1
	1h08_+0200/roddyExecutionStore/exec_170623_111000536_otproddy_WGS/r170
	623_111000536_XI061_9EP29_snvDeepAnnotation.e14973826
    group_list = B080
    Hold_Types = output1
    Join_Path = oe
    Keep_Files = n
    Mail_Points = a
    mtime = Fri Jun 23 11:10:25 2017
    Output_Path = tbi-pbs4:/icgc/dkfzlsdf/project/Xintern/XI061_ependymoma/seq
	uencing/whole_genome_sequencing/view-by-pid/XI061_9EP29/snv_results/pa
	ired/tumor_blood/results_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_
	11h08_+0200/roddyExecutionStore/exec_170623_111000536_otproddy_WGS/r17
	0623_111000536_XI061_9EP29_snvDeepAnnotation.o14973826
    Priority = 0
    qtime = Fri Jun 23 11:10:25 2017
    Rerunable = True
    Resource_List.mem = 4096mb
    Resource_List.nodect = 1
    Resource_List.nodes = 1:ppn=3
    Resource_List.walltime = 04:00:00
    euser = otproddy
    egroup = B080
    queue_type = E
    submit_args = -N r170623_111000536_XI061_9EP29_snvDeepAnnotation -o /icgc/
	dkfzlsdf/project/Xintern/XI061_ependymoma/sequencing/whole_genome_sequ
	encing/view-by-pid/XI061_9EP29/snv_results/paired/tumor_blood/results_
	SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_11h08_+0200/roddyExecutio
	nStore/exec_170623_111000536_otproddy_WGS -j oe -W group_list=B080 -W 
	umask=007 -l mem=4096m -l nodes=1:ppn=3 -l walltime=4:00:00 -W depend=
	afterok:14973825.tbi-pbs-ng.inet.dkfz-heidelberg.de -v WRAPPED_SCRIPT=
	/icgc/dkfzlsdf/project/Xintern/XI061_ependymoma/sequencing/whole_genom
	e_sequencing/view-by-pid/XI061_9EP29/snv_results/paired/tumor_blood/re
	sults_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_11h08_+0200/roddyEx
	ecutionStore/exec_170623_111000536_otproddy_WGS/analysisTools/tools/vc
	f_pipeAnnotator.sh,
	PARAMETER_FILE=/icgc/dkfzlsdf/project/Xintern/XI061_ependymoma/sequen
	cing/whole_genome_sequencing/view-by-pid/XI061_9EP29/snv_results/paire
	d/tumor_blood/results_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_11h
	08_+0200/roddyExecutionStore/exec_170623_111000536_otproddy_WGS/r17062
	3_111000536_XI061_9EP29_snvDeepAnnotation_61.parameters /icgc/dkfzlsdf
	/project/Xintern/XI061_ependymoma/sequencing/whole_genome_sequencing/v
	iew-by-pid/XI061_9EP29/snv_results/paired/tumor_blood/results_SNVCalli
	ngWorkflow-1.0.166-1_v1_0_2017-06-23_11h08_+0200/roddyExecutionStore/e
	xec_170623_111000536_otproddy_WGS/analysisTools/roddyTools/wrapInScrip
	t.sh
    umask = 7
    fault_tolerant = False
    job_radix = 0
    submit_host = tbi-pbs4.inet.dkfz-heidelberg.de
    request_version = 1

Job Id: 14973827.tbi-pbs-ng.inet.dkfz-heidelberg.de
    Job_Name = r170623_111000536_XI061_9EP29_snvFilter
    Job_Owner = otproddy@tbi-pbs4.inet.dkfz-heidelberg.de
    job_state = H
    queue = long
    server = tbi-pbs-ng.inet.dkfz-heidelberg.de
    Checkpoint = u
    ctime = Fri Jun 23 11:10:25 2017
    depend = afterok:14973826.tbi-pbs-ng.inet.dkfz-heidelberg.de:14973824.tbi-
	pbs-ng.inet.dkfz-heidelberg.de
    Error_Path = tbi-pbs4:/icgc/dkfzlsdf/project/Xintern/XI061_ependymoma/sequ
	encing/whole_genome_sequencing/view-by-pid/XI061_9EP29/snv_results/pai
	red/tumor_blood/results_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_1
	1h08_+0200/roddyExecutionStore/exec_170623_111000536_otproddy_WGS/r170
	623_111000536_XI061_9EP29_snvFilter.e14973827
    group_list = B080
    Hold_Types = output1
    Join_Path = oe
    Keep_Files = n
    Mail_Points = a
    mtime = Fri Jun 23 11:10:25 2017
    Output_Path = tbi-pbs4:/icgc/dkfzlsdf/project/Xintern/XI061_ependymoma/seq
	uencing/whole_genome_sequencing/view-by-pid/XI061_9EP29/snv_results/pa
	ired/tumor_blood/results_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_
	11h08_+0200/roddyExecutionStore/exec_170623_111000536_otproddy_WGS/r17
	0623_111000536_XI061_9EP29_snvFilter.o14973827
    Priority = 0
    qtime = Fri Jun 23 11:10:25 2017
    Rerunable = True
    Resource_List.mem = 4096mb
    Resource_List.nodect = 1
    Resource_List.nodes = 1:ppn=1
    Resource_List.walltime = 04:00:00
    euser = otproddy
    egroup = B080
    queue_type = E
    submit_args = -N r170623_111000536_XI061_9EP29_snvFilter -o /icgc/dkfzlsdf
	/project/Xintern/XI061_ependymoma/sequencing/whole_genome_sequencing/v
	iew-by-pid/XI061_9EP29/snv_results/paired/tumor_blood/results_SNVCalli
	ngWorkflow-1.0.166-1_v1_0_2017-06-23_11h08_+0200/roddyExecutionStore/e
	xec_170623_111000536_otproddy_WGS -j oe -W group_list=B080 -W umask=00
	7 -l mem=4096m -l nodes=1:ppn=1 -l walltime=4:00:00 -W depend=afterok:
	14973826.tbi-pbs-ng.inet.dkfz-heidelberg.de:14973824.tbi-pbs-ng.inet.d
	kfz-heidelberg.de -v WRAPPED_SCRIPT=/icgc/dkfzlsdf/project/Xintern/XI0
	61_ependymoma/sequencing/whole_genome_sequencing/view-by-pid/XI061_9EP
	29/snv_results/paired/tumor_blood/results_SNVCallingWorkflow-1.0.166-1
	_v1_0_2017-06-23_11h08_+0200/roddyExecutionStore/exec_170623_111000536
	_otproddy_WGS/analysisTools/snvPipeline/filter_vcf.sh,
	PARAMETER_FILE=/icgc/dkfzlsdf/project/Xintern/XI061_ependymoma/sequen
	cing/whole_genome_sequencing/view-by-pid/XI061_9EP29/snv_results/paire
	d/tumor_blood/results_SNVCallingWorkflow-1.0.166-1_v1_0_2017-06-23_11h
	08_+0200/roddyExecutionStore/exec_170623_111000536_otproddy_WGS/r17062
	3_111000536_XI061_9EP29_snvFilter_62.parameters /icgc/dkfzlsdf/project
	/Xintern/XI061_ependymoma/sequencing/whole_genome_sequencing/view-by-p
	id/XI061_9EP29/snv_results/paired/tumor_blood/results_SNVCallingWorkfl
	ow-1.0.166-1_v1_0_2017-06-23_11h08_+0200/roddyExecutionStore/exec_1706
	23_111000536_otproddy_WGS/analysisTools/roddyTools/wrapInScript.sh
    umask = 7
    fault_tolerant = False
    job_radix = 0
    submit_host = tbi-pbs4.inet.dkfz-heidelberg.de
    request_version = 1

"""

    @Test
    void testReadQstatOutput() throws Exception {
        TestExecutionService executionService = new TestExecutionService("", "")
        PBSJobManager jm = new PBSJobManager(executionService, JobManagerOptions.create()
                .setCreateDaemon(false)
                .setTrackUserJobsOnly(true)
                .build())

        Method method = jm.getClass().getDeclaredMethod("readQstatOutput", String);
        method.setAccessible(true);

        Map<String, Map<String, String>> qstatReaderResultOutput1 = (Map<String, Map<String, String>>) method.invoke(jm, output1)
        Map<String, Map<String, String>> qstatReaderResultOutput2 = (Map<String, Map<String, String>>) method.invoke(jm, output2)

        assert qstatReaderResultOutput1.size() == 4
        assert qstatReaderResultOutput2.size() == 2
    }

}
