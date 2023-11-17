package de.dkfz.roddy.execution

import spock.lang.Specification

import java.nio.file.Paths


class ExecutableTest extends Specification {
    
    def "GetExecutablePath"() {
        given:
        Executable executable = new Executable(Paths.get("a"))
        expect:
        executable.executablePath == Paths.get("a")
        executable.md5 == Optional.empty()
    }

    def "throw with null path"() {
        when:
        new Executable(null)
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "ToCommandSegmentList"() {
        given:
        Executable executable = new Executable(
                Paths.get("a"),
                "595f44fec1e92a71d3e9e77456ba80d1")
        expect:
        executable.md5 == Optional.of("595f44fec1e92a71d3e9e77456ba80d1")
        executable.toCommandSegmentList(false) == ["a"]
        executable.toCommandSegmentList(true) == [Paths.get("a").
                toAbsolutePath().toString()]
    }

    def "InvalidMd5"() {
        when:
        new Executable(Paths.get("a"), "")
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "ExecutableHashcode"() {
        given:
        Executable executable1 = new Executable(Paths.get("somePath"))
        Executable executable2 = new Executable(Paths.get("somePath"))
        Executable executable3 = new Executable(Paths.get("otherPath"))
        expect:
        executable1.hashCode() == executable2.hashCode()
        executable2.hashCode() != executable3.hashCode()
        executable1.hashCode() != executable3.hashCode()
    }

    def "ExecutableEquals"() {
        given:
        Executable executable1 = new Executable(Paths.get("somePath"))
        Executable executable2 = new Executable(Paths.get("somePath"))
        Executable executable3 = new Executable(Paths.get("otherPath"))
        expect:
        executable1 == executable2
        executable2 != executable3
        executable1 != executable3
    }

}