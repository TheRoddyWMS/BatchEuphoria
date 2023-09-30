package de.dkfz.roddy.execution

import spock.lang.Specification

import java.nio.file.Paths


class ApptainerCommandBuilderTest extends Specification {

    def "command without paths"() {
        given:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([], [])
        expect:
        builder.command.toList() == ["apptainer", "exec"]
    }

    def "command with duplicate paths on same target"() {
        given:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),

        ], [])
        expect:
        builder.command.toList() == [
                "apptainer", "exec",
                "-B", "/a/b/c:/a/b/c:ro"]
    }

    def "command with duplicate paths on same target more accessible"() {
        given:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RW),

        ], [])
        expect:
        builder.command.toList() == [
                "apptainer", "exec",
                "-B", "/a/b/c:/a/b/c:rw"]
    }

    def "command with duplicate paths on other target more accessible"() {
        given:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c1"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c2"), BindSpec.Mode.RW),

        ], [])
        expect:
        builder.command.toList() == [
                "apptainer", "exec",
                "-B", "/a/b/c:/a/b/c1:ro",
                "-B", "/a/b/c:/a/b/c2:rw"]
    }

    def "command with superpath"() {
        given:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b"), Paths.get("/a/b"), BindSpec.Mode.RO),

        ], [])
        expect:
        builder.command.toList() == [
                "apptainer", "exec",
                // Don't attempt to solve such complex situations.
                "-B", "/a/b:/a/b:ro",
                "-B", "/a/b/c:/a/b/c:ro",
        ]
    }


    def "command with superpath writable"() {
        given:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b"), Paths.get("/a/b"), BindSpec.Mode.RW),

        ], [])
        expect:
        builder.command.toList() == [
                "apptainer", "exec",
                "-B", "/a/b:/a/b:rw",
                "-B", "/a/b/c:/a/b/c:ro"
        ]
    }

    def "different sources mounted to same target"() {
        when:
        ApptainerCommandBuilder builder = new ApptainerCommandBuilder([
                new BindSpec(Paths.get("/a/b/c1"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b/c2"), Paths.get("/a/b/c"), BindSpec.Mode.RW),
        ], [])
        then:
        final IllegalArgumentException exception = thrown()

    }


}
