package de.dkfz.roddy.execution

import spock.lang.Specification

import java.nio.file.Paths


class ApptainerCommandBuilderTest extends Specification {

    def "command without paths"() {
        given:
        ApptainerCommandBuilder builder = ApptainerCommandBuilder.create()
        expect:
        builder.build("image").toCommandSegmentList() == ["apptainer", "exec", "image"]
    }

    def "command with duplicate paths on same target"() {
        given:
        ApptainerCommandBuilder builder = ApptainerCommandBuilder.create().withAddedBindingSpecs([
            new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
            new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
        ])
        expect:
        builder.build("image").toCommandSegmentList() == [
                "apptainer", "exec",
                "-B", "/a/b/c:/a/b/c:ro",
                "image"]
    }

    def "command with duplicate paths on same target more accessible"() {
        given:
        ApptainerCommandBuilder builder = ApptainerCommandBuilder.create().withAddedBindingSpecs([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RW),
        ])
        expect:
        builder.build("someImage").toCommandSegmentList() == [
                "apptainer", "exec",
                "-B", "/a/b/c:/a/b/c:rw",
                "someImage"]
    }

    def "command with duplicate paths on other target more accessible"() {
        given:
        ApptainerCommandBuilder builder = ApptainerCommandBuilder.create().withAddedBindingSpecs([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c1"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c2"), BindSpec.Mode.RW),
        ])
        expect:
        builder.build("image").toCommandSegmentList() == [
                "apptainer", "exec",
                "-B", "/a/b/c:/a/b/c1:ro",
                "-B", "/a/b/c:/a/b/c2:rw",
                "image"]
    }

    def "command with superpath"() {
        given:
        ApptainerCommandBuilder builder = ApptainerCommandBuilder.create().withAddedBindingSpecs([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b"), Paths.get("/a/b"), BindSpec.Mode.RO),
        ])
        expect:
        // Don't attempt to solve such complex situations: Although /a/b is a superpath of /a/b/c
        // and both are ro, we do not unify them to just /a/b:ro.
        builder.build("image").toCommandSegmentList() == [
                "apptainer", "exec",
                "-B", "/a/b:/a/b:ro",
                "-B", "/a/b/c:/a/b/c:ro",
                "image"
        ]
    }


    def "command with superpath writable"() {
        given:
        ApptainerCommandBuilder builder = ApptainerCommandBuilder.create().withAddedBindingSpecs([
                new BindSpec(Paths.get("/a/b/c"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b"), Paths.get("/a/b"), BindSpec.Mode.RW),
        ])
        expect:
        builder.build("someImage").toCommandSegmentList() == [
                "apptainer", "exec",
                "-B", "/a/b:/a/b:rw",
                "-B", "/a/b/c:/a/b/c:ro",
                "someImage"
        ]
    }

    def "different sources mounted to same target"() {
        when:
        ApptainerCommandBuilder builder = ApptainerCommandBuilder.create().withAddedBindingSpecs([
                new BindSpec(Paths.get("/a/b/c1"), Paths.get("/a/b/c"), BindSpec.Mode.RO),
                new BindSpec(Paths.get("/a/b/c2"), Paths.get("/a/b/c"), BindSpec.Mode.RW),
        ])
        builder.build("image")
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "error if missing image name"() {
        when:
        ApptainerCommandBuilder builder = ApptainerCommandBuilder.create()
        builder.build()
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "error if invalid image name"() {
        when:
        ApptainerCommandBuilder builder = ApptainerCommandBuilder.create()
        builder.build("")
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "error if invalid image name during construction"() {
        when:
        ApptainerCommandBuilder.
                create().
                withImageId("")
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "error if null mode during construction"() {
        when:
        ApptainerCommandBuilder.create().withMode(null)
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "error if null engine args during construction"() {
        when:
        ApptainerCommandBuilder.create().withAddedEngineArgs(null)
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "error if null binding specs during construction"() {
        when:
        ApptainerCommandBuilder.create().withAddedBindingSpecs(null)
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "error if null executable"() {
        when:
        ApptainerCommandBuilder.create().withApptainerExecutable(null)
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "error if null added exported env vars"() {
        when:
        ApptainerCommandBuilder.create().withCopiedEnvironmentVariables(null)
        then:
        final IllegalArgumentException exception = thrown()
    }

    def "use class-level default image name and apptainer executable"() {
        when:
        ApptainerCommandBuilder builder = ApptainerCommandBuilder
            .create()
            .withApptainerExecutable(Paths.get("/bin/executable"))
            .withImageId("image")
        then:
        builder.build().toCommandSegmentList() == [
                "/bin/executable", "exec",
                "image"
        ]
    }

    def "copy environment variables into container"() {
        when:
        ApptainerCommandBuilder builder = ApptainerCommandBuilder
            .create()
            .withAddedEngineArgs(["--contain"])
            .withCopiedEnvironmentVariables(["a"])   // Add variables incrementally.
            .withCopiedEnvironmentVariables(["b"])
            .withAddedEnvironmentVariables(["a": "\$c"])    // Explicit override of variable value.
        then:
        builder.build("someImage").toCommandSegmentList() == [
                "apptainer", "exec",
                "--env", "a=\$c",
                "--env", "b=\$b",
                "--contain", "someImage"
        ]
    }

}
