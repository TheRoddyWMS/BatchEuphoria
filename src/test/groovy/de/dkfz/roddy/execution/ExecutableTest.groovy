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

    def "ToList"() {
        given:
        Executable executable = new Executable(
                Paths.get("a"),
                "595f44fec1e92a71d3e9e77456ba80d1")
        expect:
        executable.md5 == Optional.of("595f44fec1e92a71d3e9e77456ba80d1")
        executable.toList(false) == ["a"]
        executable.toList(true) == [Paths.get("a").toAbsolutePath().toString()]
    }

    def "InvalidMd5"() {
        when:
        new Executable(Paths.get("a"), "")
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "NullPath"() {
        when:
        new Executable(null)
        then:
        final IllegalArgumentException exception = thrown()
    }
}
