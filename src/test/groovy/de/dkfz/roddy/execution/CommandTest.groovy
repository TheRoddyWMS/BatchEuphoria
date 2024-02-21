package de.dkfz.roddy.execution

import spock.lang.Specification

import java.nio.file.Paths

class CommandTest extends Specification {

    def "throw with null string argument"() {
        when:
        new Command(new Executable(Paths.get("somePath")), [null as String])
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
        Command command4 = new Command(new Executable(Paths.get("somePath")), ["otherArg"])
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
        Command command4 = new Command(new Executable(Paths.get("somePath")), ["otherArg"])
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
        new Command(new Executable(Paths.get("somePath")), null as List<String>)
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
                Paths.get("someOtherPath")), ["a", "b", "c"])
        expect:
        commandWithoutArgs.toCommandSegmentList() ==
                ["somePath"]
        commandWithoutArgs.toCommandSegmentList() ==
                [Paths.get("somePath").toString()]
        commandWithArgs.toCommandSegmentList() ==
                ["someOtherPath", "a", "b", "c"]
        commandWithArgs.toCommandSegmentList() ==
                [Paths.get("someOtherPath").toString(), "a", "b", "c"]
    }

    def "CliAppendCommandExecutable"() {
        given:
        Command command1 = new Command(new Executable(Paths.get("strace")), ["stracearg1", "--"])
        Command command2 = new Command(new Executable(Paths.get("someTool")), ["toolarg1"])
        Executable executable = new Executable(Paths.get("executableX"))
        expect:
        command1.cliAppend(command2).toCommandSegmentList() == [
                "strace", "stracearg1", "--", "someTool", "toolarg1"
        ]
        command1.cliAppend(command2, true).toCommandSegmentList() == [
                "strace", "stracearg1", "--", "someTool toolarg1"
        ]
        command1.cliAppend(executable).toCommandSegmentList() == [
                "strace", "stracearg1", "--", "executableX"
        ]
    }

    def "CliAppendCode"() {
        given:
        Code code1 = new Code("echo hallo; sleep 50;")
        expect:
        new Command(new Executable(Paths.get("cat")), ["-"]).cliAppend(
                code1,
                new Executable(Paths.get("/bin/tcsh")),
                "prefix",
                "test").toCommandString() == """\
                    |#!/bin/tcsh
                    |cat - <<prefix_test
                    |#!/bin/bash
                    |echo hallo; sleep 50;
                    |prefix_test
                    |""".stripMargin()
    }

}
