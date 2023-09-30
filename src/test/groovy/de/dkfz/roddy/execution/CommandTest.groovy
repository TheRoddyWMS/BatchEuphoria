package de.dkfz.roddy.execution

import spock.lang.Specification

import java.nio.file.Paths


class CommandTest extends Specification {


    def "GetExecutablePath"() {
        given:
        Command command = new Command(new Executable(Paths.get("somePath")), [])
        expect:
        command.executablePath == Paths.get("somePath")
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
        commandWithoutArgs.toList() ==
                ["somePath"]
        commandWithoutArgs.toList(true) ==
                [Paths.get("somePath").toAbsolutePath().toString()]
        commandWithArgs.toList() ==
                ["someOtherPath", "a", "b", "c"]
        commandWithArgs.toList(true) ==
                [Paths.get("someOtherPath").toAbsolutePath().toString(), "a", "b", "c"]
    }

    def "CliAppendCommandExecutable"() {
        given:
        Command command1 = new Command(new Executable(Paths.get("strace")), ["stracearg1", "--"])
        Command command2 = new Command(new Executable(Paths.get("someTool")), ["toolarg1"])
        Executable executable = new Executable(Paths.get("executableX"))
        expect:
        command1.cliAppend(command2).toList() == [
                "strace", "stracearg1", "--", "someTool", "toolarg1"
        ]
        command1.cliAppend(executable).toList() == [
                "strace", "stracearg1", "--", "executableX"
        ]
    }

    def "CliAppendCode"() {
        given:
        Code code1 = new Code("echo hallo; sleep 50;")
        Executable executable = new Executable(Paths.get("strace"))
        expect:
        new Command(executable).cliAppend(
                code1,
                false,
                Paths.get("/bin/bash"),
                "prefix",
                "test").code == """\
                |strace <<prefix_test
                |#!/bin/bash
                |echo hallo; sleep 50;
                |prefix_test
                |""".stripMargin()
    }

}