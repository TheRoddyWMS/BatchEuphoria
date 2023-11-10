package de.dkfz.roddy.execution


import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths

class BindSpecTest extends Specification {

    private Path hostPath = Paths.get("hostPath")
    private Path contPath = Paths.get("contPath")

    def "mode creation from string"() {
        expect:
        BindSpec.Mode.from("rO") == BindSpec.Mode.RO
        BindSpec.Mode.from("Rw") == BindSpec.Mode.RW
    }

    def "initialization"() {
        when:
        BindSpec spec = new BindSpec(hostPath)
        then:
        spec.containerPath == spec.hostPath
        spec.mode == BindSpec.Mode.RO
    }

    def "bind options"() {
        expect:
        new BindSpec(hostPath, hostPath).toBindOption() == "hostPath:hostPath:ro"

        new BindSpec(hostPath, contPath).toBindOption() == "hostPath:contPath:ro"

        new BindSpec(hostPath).toBindOption() == "hostPath:hostPath:ro"

        new BindSpec(hostPath, null, BindSpec.Mode.RW).toBindOption() == "hostPath:hostPath:rw"
    }

    def "bind spec null check path"() {
        when:
        new BindSpec(null, null)
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "bind spec null check mode"() {
        when:
        new BindSpec(hostPath, null, null)
        then:
        final IllegalArgumentException exception = thrown()

    }




}
