package de.dkfz.roddy.execution

import spock.lang.Specification

import java.nio.file.Paths


class ApptainerCommandBuilderTest extends Specification {

    def "command without paths"() {
        given:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([], [])
        expect:
        builder.build("image").toList() == ["apptainer", "exec", "image"]
    }

    def "command with duplicate paths on same target"() {
        given:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),

        ], [])
        expect:
        builder.build("image").toList() == [
                "apptainer", "exec",
                "-B", "/a/b/c:ro",
                "image"]
    }

    def "command with duplicate paths on same target more accessible"() {
        given:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RW),

        ], [])
        expect:
        builder.build("someImage").toList() == [
                "apptainer", "exec",
                "-B", "/a/b/c:rw",
                "someImage"]
    }

    def "command with duplicate paths on other target more accessible"() {
        given:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c1"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c2"), BindSpec.Mode.RW),

        ], [])
        expect:
        builder.build("image").toList() == [
                "apptainer", "exec",
                "-B", "/a/b/c:/a/b/c1:ro",
                "-B", "/a/b/c:/a/b/c2:rw",
                "image"]
    }

    def "command with superpath"() {
        given:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b"), Paths.get("/a/b"), BindSpec.Mode.RO),

        ], [])
        expect:
        // Don't attempt to solve such complex situations.
        builder.build("image").toList() == [
                "apptainer", "exec",
                "-B", "/a/b:ro",
                "-B", "/a/b/c:ro",
                "image"
        ]
    }


    def "command with superpath writable"() {
        given:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b"), Paths.get("/a/b"), BindSpec.Mode.RW),

        ], [])
        expect:
        builder.build("someImage").toList() == [
                "apptainer", "exec",
                "-B", "/a/b:rw",
                "-B", "/a/b/c:ro",
                "someImage"
        ]
    }

    def "different sources mounted to same target"() {
        when:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([
                new BindSpec(Paths.get("/a/b/c1"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b/c2"), Paths.get("/a/b/c"), BindSpec.Mode.RW),
        ], [])
        builder.build("image")
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "error if missing image name"() {
        when:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([], [])
        builder.build()
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "error if invalid image name"() {
        when:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([], [])
        builder.build("")
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "error if invalid image name during construction"() {
        when:
        ApptainerCommandBuilder builder =
                new ApptainerCommandBuilder([], [], Paths.get("/bin/bash"), "")
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "use class-level default image name and apptainer executable"() {
        when:
        ApptainerCommandBuilder builder =
                new ApptainerCommandBuilder([], [], Paths.get("/bin/executable"), "image")
        then:
        builder.build().toList() == [
                "/bin/executable", "exec",
                "image"
        ]
    }


}
