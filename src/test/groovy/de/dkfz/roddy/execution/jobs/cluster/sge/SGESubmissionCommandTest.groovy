package de.dkfz.roddy.execution.jobs.cluster.sge

import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.config.EmptyResourceSet
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.tools.BashInterpreter
import de.dkfz.roddy.tools.EscapableString
import spock.lang.Specification

import static de.dkfz.roddy.tools.EscapableString.Shortcuts.*

class SGESubmissionCommandTest extends Specification {

    private BatchEuphoriaJobManager manager =
            new SGEJobManager(new TestExecutionService("testUser",
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

    private SGESubmissionCommand command1 =
        new SGESubmissionCommand(manager,
                                 job1,
                                 e("jobName"),
                                 [] as List<ProcessingParameters>,
                                 [a: e("val you"), another: u("value")] as Map<String, EscapableString>)

    private SGESubmissionCommand command2 =
        new SGESubmissionCommand(manager,
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
        command1.workingDirectoryParameter == c(u("-wd "), u("\$HOME"))
        command2.workingDirectoryParameter == c(u("-wd "), e(u("someDir")))
    }

    def "GetLoggingParameter"() {
        expect:
        command1.getLoggingParameter(JobLog.none()) ==
            u("-o /dev/null -e /dev/null")
        command1.getLoggingParameter(JobLog.toOneFile(new File("out/{JOB_ID}"))) ==
            c([u("-j y"), u(" -o "), e(u("out/\\\$JOB_ID"))])
        command1.getLoggingParameter(JobLog.toSeparateStdoutAndError(
                new File("out/{JOB_ID}"),
                new File("err.{JOB_ID}"))).interpreted(BashInterpreter.instance) ==
                '-o out/\\\\\\\$JOB_ID -e err.\\\\\\\$JOB_ID'
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
        command1.dependencyParameterName == "-hold_jid"
    }

    def "GetAdditionalCommandParameters"() {
        expect:
        command1.additionalCommandParameters == u(" -S /bin/bash ")
    }

    def "AssembleVariableExportParameters"() {
        expect:
        command1.assembleVariableExportParameters().interpreted(BashInterpreter.instance) ==
                "-v a\\=val\\ you,another\\=value"
//                "-v fromJob\\=val,a\\=val\\ you,another\\=value"
    }

}
