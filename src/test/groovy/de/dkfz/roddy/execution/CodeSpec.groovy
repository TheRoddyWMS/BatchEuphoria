package de.dkfz.roddy.execution

import spock.lang.Specification

import java.nio.file.Paths
import static de.dkfz.roddy.execution.EscapableString.*

class CodeSpec extends Specification {

    def "GetInterpreter"() {
        given:
        Code code1 = new Code("echo hallo; sleep 50;")
        Code code2 = new Code("println(\"hallo\")",
                              Paths.get("/usr/bin/python3"))
        expect:
        code1.code == u("echo hallo; sleep 50;")
        code1.interpreter == new Executable(Paths.get("/bin/bash"))
        code2.code == u("println(\"hallo\")")
        code2.interpreter == new Executable(Paths.get("/usr/bin/python3"))
    }

    def "throw with null code"() {
        when:
        new Code(null as AnyEscapableString)
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "throw with null interpreter"() {
        when:
        new Code("echo hallo; sleep 50;", null)
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "create command string"() {
        when:
        Code code1 = new Code("echo hallo; sleep 50;",
                              new Command(new Executable(Paths.get("/bin/bash")),
                                          [u("-xe")] as List<AnyEscapableString>))
        then:
        forBash(code1.toEscapableString(true)) == """\
            |#!/bin/bash -xe
            |echo hallo; sleep 50;
            |""".stripMargin()
    }

}
