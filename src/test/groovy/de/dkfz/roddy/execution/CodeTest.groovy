package de.dkfz.roddy.execution

import spock.lang.Specification

import java.nio.file.Paths


class CodeTest extends Specification {

    def "GetInterpreter"() {
        given:
        Code code1 = new Code("echo hallo; sleep 50;")
        Code code2 = new Code("println(\"hallo\")", Paths.get("/usr/bin/python3"))
        expect:
        code1.code == "echo hallo; sleep 50;"
        code1.interpreter == Paths.get("/bin/bash")
        code2.code == "println(\"hallo\")"
        code2.interpreter == Paths.get("/usr/bin/python3")
    }

    def "throw with null code"() {
        when:
        new Code(null)
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "throw with null interpreter"() {
        when:
        new Code("echo hallo; sleep 50;", null)
        then:
        final IllegalArgumentException exception = thrown()
    }

}
