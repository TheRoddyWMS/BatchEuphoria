package de.dkfz.roddy.execution

import de.dkfz.roddy.tools.AnyEscapableString
import de.dkfz.roddy.tools.BashInterpreter
import spock.lang.Specification

import java.nio.file.Paths
import static de.dkfz.roddy.tools.EscapableString.*

class CommandSpec extends Specification {

    def "throw with null string argument"() {
        when:
        new Command(new Executable(Paths.get("somePath")),
                    [null as AnyEscapableString])
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "GetExecutablePath"() {
        given:
        Command command = new Command(new Executable(Paths.get("somePath")), [])
        expect:
        command.executablePath == Paths.get("somePath")
    }

    def "CommandHashCode"() {
        given:
        Command command1 = new Command(new Executable(Paths.get("somePath")), [])
        Command command2 = new Command(new Executable(Paths.get("somePath")), [])
        Command command3 = new Command(new Executable(Paths.get("otherPath")), [])
        Command command4 = new Command(new Executable(
                Paths.get("somePath")),
                [u("otherArg")])
        expect:
        command1.hashCode() == command2.hashCode()
        command2.hashCode() != command3.hashCode()
        command1.hashCode() != command4.hashCode()
    }

    def "CommandEquals"() {
        given:
        Command command1 = new Command(new Executable(Paths.get("somePath")), [])
        Command command2 = new Command(new Executable(Paths.get("somePath")), [])
        Command command3 = new Command(new Executable(Paths.get("otherPath")), [])
        Command command4 = new Command(new Executable(Paths.get("somePath")),
                                       [u("otherArg")])
        expect:
        command1 == command2
        command2 != command3
        command1 != command4
    }

    def "throw with null Executable"() {
        when:
        new Command(null as Executable, [])
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "throw with null argument list"() {
        when:
        new Command(new Executable(Paths.get("somePath")), null as List<AnyEscapableString>)
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "GetExecutable"() {
        given:
        Command command = new Command(new Executable(Paths.get("somePath")), [])
        expect:
        command.executable == new Executable(Paths.get("somePath"))
    }

    def "ToList"() {
        given:
        Command commandWithoutArgs = new Command(new Executable(
                Paths.get("somePath")), [])
        Command commandWithArgs = new Command(new Executable(
                Paths.get("someOtherPath")), [u("a"), u("b"), u("c")])
        expect:
        commandWithoutArgs.toCommandSegmentList() ==
                ["somePath"].collect { u(it) }
        commandWithoutArgs.toCommandSegmentList() ==
                [Paths.get("somePath").toString()].collect { u(it) }
        commandWithArgs.toCommandSegmentList() ==
                ["someOtherPath", "a", "b", "c"].collect { u(it) }
        commandWithArgs.toCommandSegmentList() ==
                [Paths.get("someOtherPath").toString(), "a", "b", "c"].collect { u(it) }
    }

    def "CliAppendCommandExecutable"() {
        given:
        Command command1 = new Command(new Executable(Paths.get("strace")),
                                       ["stracearg1", "--"].collect { u(it) })
        Command command2 = new Command(new Executable(Paths.get("someTool")),
                                       ["toolarg1"].collect { u(it) })
        Executable executable = new Executable(Paths.get("executableX"))

        expect:
        command1.cliAppend(command2).toCommandSegmentList() == [
                "strace", "stracearg1", "--", "someTool", "toolarg1"
        ].collect { u(it) } as List<AnyEscapableString>

        command1.cliAppend(command2, true).toCommandSegmentList() == [
                u("strace"), u("stracearg1"), u("--"),
                c(e("someTool"), e(" "), e("toolarg1"))
        ] as List<AnyEscapableString>

        command1.cliAppend(executable).toCommandSegmentList() == [
                "strace", "stracearg1", "--", "executableX"
        ].collect { u(it)} as List<AnyEscapableString>
    }

    def "CliAppendCode"() {
        given:
        Code code1 = new Code("echo hallo; sleep 50;")
        AnyEscapableString result =
                new Command(new Executable(Paths.get("cat")), ["-"].collect { u(it) }).
                        cliAppend(code1,
                                  new Executable(Paths.get("/bin/bash")),
                                  "prefix",
                                  "test").toEscapableString()
        expect:
        BashInterpreter.instance.interpret(result) == """\
                    |#!/bin/bash
                    |cat - <<prefix_test
                    |#!/bin/bash
                    |echo hallo; sleep 50;
                    |prefix_test
                    |""".stripMargin()
    }

}
