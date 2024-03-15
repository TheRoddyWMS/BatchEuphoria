package de.dkfz.roddy.execution

import spock.lang.Specification

import static de.dkfz.roddy.execution.EscapableString.*


class EscapableStringSpec extends Specification {
    
    EscapableStringInterpreter interpreter = BashInterpreter.instance

    def "ToBashString no escaping"() {
        when:
        def escapedString = e("test")
        def unescapedString = u("test")
        def concatenatedString = c([escapedString, unescapedString])

        then:
        interpreter.interpret(escapedString) == "test"
        interpreter.interpret(unescapedString) == "test"
        interpreter.interpret(concatenatedString) == "testtest"
    }

    def "ToBashString with escaping and concatenate with + operator"() {
        when:
        def escapedString = e("\${someVariable}")
        def unescapedString = u("varName=")
        def concatenatedString1 = unescapedString + escapedString
        def concatenatedString2 = unescapedString + "someString"
        def concatenatedString3 = c(c(escapedString, unescapedString), unescapedString, escapedString)

        then:
        interpreter.interpret(escapedString) == "\\\${someVariable}"
        interpreter.interpret(unescapedString) == "varName="
        interpreter.interpret(concatenatedString1) == "varName=\\\${someVariable}"
        interpreter.interpret(concatenatedString2) == "varName=someString"
        interpreter.interpret(concatenatedString3) == "\\\${someVariable}varName=varName=\\\${someVariable}"
    }

    def "String representation"() {
        when:
        def unescapedString = u("test")

        then:
        interpreter.interpret(unescapedString) == "test"
        unescapedString.escaped.toString() == "e(u(test))"
        unescapedString.escaped.escaped.toString() == "e(e(u(test)))"
    }

    def "equals"() {
        when:
        def unescapedString = u("test")
        def unescapedString2 = u("test")
        def escapedString = e("test")
        def escapedString2 = e("test")
        def concatenatedString0 = c()
        def concatenatedString1 = c([unescapedString, unescapedString2])
        def concatenatedString2 = c([unescapedString, unescapedString2])
        def concatenatedString3 = c([unescapedString, unescapedString2, unescapedString])
        def concatenatedString4 = c([unescapedString, unescapedString2, unescapedString])

        then:
        unescapedString == unescapedString2
        unescapedString != escapedString
        unescapedString != concatenatedString1
        e(unescapedString) == escapedString2
        concatenatedString0 == c()
        concatenatedString1 == concatenatedString2
        concatenatedString1 != concatenatedString3
        concatenatedString3 == concatenatedString4
        concatenatedString1 != unescapedString
        concatenatedString1 != c()
    }

    def "Stack escapes"() {
        when:
        def unescapedString = u("\${someVariable}")

        then:
        interpreter.interpret(unescapedString) == "\${someVariable}"
        unescapedString.escaped == e("\${someVariable}")
        interpreter.interpret(unescapedString.escaped) == "\\\${someVariable}"
        unescapedString.escaped.escaped == e(e(unescapedString))
        interpreter.interpret(unescapedString.escaped.escaped) == "\\\\\\\${someVariable}"
    }

    def "Simplify nested concatenated strings"() {
        when:
        def conc0 = c()
        def conc1 = c([u("test1"), u("test2"), c()]) // c() is ignored
        def conc2 = c([conc1, e("test3")])
        def conc3 = c([conc2, u("test4")])
        def conc4 = c(conc0)
        def conc5 = c(c(c()))
        def conc6 = c() + u("hallo")

        then:
        conc0 == new ConcatenatedString([])
        conc2 == c([u("test1"), u("test2"), e("test3")])
        conc3 == c([u("test1"), u("test2"), e("test3"), u("test4")])
        conc4 == c()
        conc5 == c()
        conc6 == c(u("hallo"))
    }

    def "Join list of AnyEscapableStrings"() {
        when:
        def list = [e("test1"), u("test2"), e("\$var")]

        then:
        join(list, " ") == c(
                e("test1"),
                u(" "),
                u("test2"),
                u(" "),
                e("\$var"))
        join(list, e(" | ")) == c(
                e("test1"),
                e(" | "),
                u("test2"),
                e(" | "),
                e("\$var"))
    }

}
