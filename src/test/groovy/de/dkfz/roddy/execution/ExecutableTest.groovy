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

    def "ToList"() {
        given:
        Executable executable = new Executable(Paths.get("a"), "abc")
        expect:
        executable.md5 == Optional.of("abc")
        executable.toList(false) == ["a"]
        executable.toList(true) == [Paths.get("a").toAbsolutePath().toString()]
    }
}
