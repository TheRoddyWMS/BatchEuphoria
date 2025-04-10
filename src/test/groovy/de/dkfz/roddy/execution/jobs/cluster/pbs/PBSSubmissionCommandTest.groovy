package de.dkfz.roddy.execution.jobs.cluster.pbs

import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.config.EmptyResourceSet
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.tools.BashInterpreter
import de.dkfz.roddy.tools.EscapableString
import spock.lang.Specification

import static de.dkfz.roddy.StringConstants.EMPTY
import static de.dkfz.roddy.tools.EscapableString.Shortcuts.*

class PBSSubmissionCommandTest extends Specification {


    private BatchEuphoriaJobManager manager =
            new PBSJobManager(new TestExecutionService("testUser",
                                                       "testHost"),
                              new JobManagerOptions())

    private BEJob job1 = new BEJob(BEJobID.newUnknown,
                                   manager,
                                   u("job1"),
                                   null,
                                   new EmptyResourceSet(),
                                   [] as Collection<BEJob>,
                                   [fromJob: u("val")] as Map<String, EscapableString>,
                                   JobLog.none(),
                                   null as File,
                                   null as EscapableString)

    private BEJob job2 = new BEJob(BEJobID.newUnknown,
                                   manager,
                                   u("job2"),
                                   null,
                                   new EmptyResourceSet(),
                                   [] as Collection<BEJob>,
                                   [:] as Map<String, EscapableString>,
                                   JobLog.none(),
                                   new File("someDir"),
                                   e("accountingName"))
    
    private PBSSubmissionCommand command1 =
        new PBSSubmissionCommand(manager,
                                 job1,
                                 e("jobName"),
                                 [] as List<ProcessingParameters>,
                                 [a: e("val you"), another: u("value")] as Map<String, EscapableString>)

    private PBSSubmissionCommand command2 =
        new PBSSubmissionCommand(manager,
                                 job2,
                                 e("jobName"),
                                 [] as List<ProcessingParameters>,
                                 [:] as Map<String, EscapableString>)

    def "GetJobNameParameter"() {
        expect:
        command1.jobNameParameter == c([u("-N "), e("jobName")])
    }

    def "GetHoldParameter"() {
        expect:
        command1.holdParameter == u("-h")
    }

    def "GetAccountNameParameter"() {
        expect:
        command1.accountNameParameter == c()
        command2.accountNameParameter == c([u("-A "), e(u("accountingName"))])
    }

    def "GetWorkingDirectoryParameter"() {
        expect:
        command1.workingDirectoryParameter == c(u("-w "), u("\$HOME"))
        command2.workingDirectoryParameter == c(u("-w "), e(u("someDir")))
    }

    def "GetLoggingParameter"() {
        expect:
        command1.getLoggingParameter(JobLog.none()) ==
            u("-k n")
        command1.getLoggingParameter(JobLog.toOneFile(new File("out/{JOB_ID}"))) ==
            c([u("-j oe"), u(" -o "), e(u("out/\\\$PBS_JOBID"))])
        command1.getLoggingParameter(JobLog.toSeparateStdoutAndError(
                new File("out/{JOB_ID}"),
                new File("err.{JOB_ID}"))).interpreted(BashInterpreter.instance) ==
                '-o out/\\\\\\\$PBS_JOBID -e err.\\\\\\\$PBS_JOBID'
    }

    def "GetEmailParameter"() {
        expect:
        command1.getEmailParameter(e("hello@world.com")) ==
            c([u(" -M "), e(u("hello@world.com"))])
        command1.getEmailParameter(null) ==
            c()
    }

    def "GetDependencyParameterName"() {
        expect:
        command1.dependencyParameterName == "afterok"
    }

    def "GetAdditionalCommandParameters"() {
        expect:
        command1.additionalCommandParameters == u(EMPTY)
    }

    def "AssembleVariableExportParameters"() {
        expect:
        command1.assembleVariableExportParameters().interpreted(BashInterpreter.instance) ==
                "-v a\\=val\\ you,another\\=value"
//                "-v fromJob\\=val,a\\=val\\ you,another\\=value"
    }

}
