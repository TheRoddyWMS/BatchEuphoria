package de.dkfz.roddy.execution


import com.google.common.base.Preconditions
import groovy.transform.AutoClone
import groovy.transform.CompileStatic

import org.jetbrains.annotations.NotNull
import org.jetbrains.annotations.Nullable

import java.nio.file.Path
import java.nio.file.Paths

import static de.dkfz.roddy.execution.EscapableString.*


@CompileStatic
class BindSpec implements Comparable<BindSpec> {

    enum Mode {
        RO("ro"),
        RW("rw");

        String stringValue

        private Mode(String value) {
            stringValue = value
        }

        String toString() { stringValue; }

        static Mode from(String value) {
            value.toUpperCase() as Mode
        }

    }

    @NotNull final Path hostPath
    final Path containerPath
    @NotNull final Mode mode

    BindSpec(@NotNull Path hostPath,
             Path containerPath = null,
             @NotNull Mode opts = Mode.RO) {
        Preconditions.checkNotNull(hostPath)
        this.hostPath = hostPath
        if (containerPath == null) {
            this.containerPath = hostPath
        } else {
            this.containerPath = containerPath
        }
        Preconditions.checkNotNull(opts)
        this.mode = opts
    }

    String toBindOption() {
        "$hostPath:$containerPath:$mode"
    }

    String toString() {
        toBindOption()
    }

    @Override
    int compareTo(BindSpec other) {
        Integer result = this.hostPath.compareTo(other.hostPath)
        if (result != 0) return result

        result = this.mode.compareTo(other.mode)
        if (result != 0) return result

        result = this.containerPath.compareTo(other.containerPath)
        if (result != 0) return result

        return 0
    }

}


@AutoClone
@CompileStatic
class ApptainerCommandBuilder {

    /** See `apptainer --help`, where it is called `Command` but there are already too many
     *  different meanings of "Command" in Roddy and BE. */
    enum Mode {
        exec,
        run
    }

    private @NotNull List<BindSpec> bindSpecifications

    private @NotNull Path apptainerExecutable = Paths.get("apptainer")

    private @NotNull List<String> engineArgs

    private @Nullable String imageId

    private @NotNull String mode

    private @Nullable Path workingDir

    private @NotNull List<String> environmentVariablesToCopy

    private @NotNull Map<String, AnyEscapableString> setEnvironmentVariables

    /** Create a command that will be wrapped into a singularity/apptainer call.
     *  For this to work all remote paths (i.e. on the execution node) need to be
     *  bound into the container. This includes
     *
     *  * input data dir
     *  * output data dir
     *  * plugin base directories
     *
     *  The whole set of directories will be deduplicated, mounts specified as both RO and RW
     *  will be bound RW, and everything appended with -B options to the singularity/apptainer
     *  command in a topologically sorted order (i.e. super-directories first).
     *
     * @param mode                   The run mode, e.g., exec, run, etc. Defaults to exec
     */
    private ApptainerCommandBuilder() {
        /** Default values for all other variables. Use the with-methods to set these. */
        this.mode = Mode.exec
        this.engineArgs = []
        this.bindSpecifications = []
        this.apptainerExecutable = Paths.get("apptainer")
        this.imageId = null
        this.workingDir = null
        this.environmentVariablesToCopy = []
        this.setEnvironmentVariables = [:]
    }

    /** Seems kind of superfluous, but this fits a bit better into the fluent-API of the builder */
    static ApptainerCommandBuilder create() {
        new ApptainerCommandBuilder()
    }

    // Methods to set the fields. More wordy, but also more structure than a long sequence
    // of constructor parameters. Groovy's support for named arguments is rudimentary and
    // clumsy (compared to Scala; e.g. no automatic name-checking).

    /** Set the execution mode ("command"; subcommand) of the apptainer call. run or exec. */
    ApptainerCommandBuilder withMode(@NotNull Mode mode) {
        Preconditions.checkNotNull(mode, "mode cannot be null")
        ApptainerCommandBuilder copy = clone()
        copy.mode = mode
        copy
    }

    /** Add container engine arguments. See apptainer run|exec --help. */
    ApptainerCommandBuilder withAddedEngineArgs(@NotNull List<String> newArgs) {
        Preconditions.checkNotNull(newArgs, "newArgs cannot be null")
        ApptainerCommandBuilder copy = clone()
        copy.engineArgs += newArgs
        copy
    }

    /** Add bind mounts. Include all directories needed to run the workload task/job. */
    ApptainerCommandBuilder withAddedBindingSpecs(@NotNull List<BindSpec> newSpecs) {
        Preconditions.checkNotNull(newSpecs, "newSpecs cannot be null")
        ApptainerCommandBuilder copy = clone()
        copy.bindSpecifications += newSpecs
        copy
    }

    /** The path to the apptainer/singularity executable on the execution side (maybe cluster). */
    ApptainerCommandBuilder withApptainerExecutable(@NotNull Path newExec) {
        Preconditions.checkNotNull(newExec, "newExec cannot be null")
        ApptainerCommandBuilder copy = clone()
        copy.apptainerExecutable = newExec
        copy
    }

    /** The path or URI of the container image to wrap the command in. */
    ApptainerCommandBuilder withImageId(String newId) {
        Preconditions.checkArgument(newId == null || newId.length() > 0,
                                    "newId cannot be null and must not be empty")
        ApptainerCommandBuilder copy = clone()
        copy.imageId = newId
        copy
    }

    /** The working directory inside the container. */
    ApptainerCommandBuilder withWorkingDir(@Nullable Path workingDir) {
        ApptainerCommandBuilder copy = clone()
        copy.workingDir = workingDir
        copy
    }

    /** List of variables that will be copied from the calling environment of the apptainer/
     *  singularity command. This will usually be e.g. LSB_JOBID or LSB_JOBNAME, but may include
     *  others to configure the program that you want to run in the container.
     */
    ApptainerCommandBuilder withCopiedEnvironmentVariables(@NotNull List<String> newNames) {
        Preconditions.checkNotNull(newNames, "newNames cannot be null")
        ApptainerCommandBuilder copy = clone()
        copy.environmentVariablesToCopy += newNames
        copy
    }

    /** Environment variables names and values that will be set on the container.
     *  Note that this will not check the syntactic (bash) correctness of the variable names
     *  or values. Also no quoting will be done, such that you can e.g. do something like
     *  ["renamedVariableName": "originalVariableValue"]
     */
    ApptainerCommandBuilder withAddedEnvironmentVariables(
            @NotNull Map<String, AnyEscapableString> newNameValues) {
        Preconditions.checkNotNull(newNameValues, "newNameValues must not be null")
        ApptainerCommandBuilder copy = clone()
        copy.setEnvironmentVariables += newNameValues
        copy
    }

    // Helpers for the build() method to create the actual Command object.

    private List<AnyEscapableString> getWorkingDirParameter() {
        if (workingDir != null) {
            [u("-W"), e(workingDir.toString())] as List<AnyEscapableString>
        } else {
            []
        }
    }

    private Map<String, AnyEscapableString> getExportedEnvironmentVariables() {
        this.environmentVariablesToCopy.collectEntries { variableName ->
            [variableName, e("\$$variableName")]
        }
    }

    /** This is similar to `getExportedLocalEnvironmentVariables()`, but here we actually set
     *  variables to (possibly new) values. Note that the values are *not* quoted. We leave it to
     *  the caller, to quote values correctly, if necessary.
     */
    private Map<String, AnyEscapableString> getExplicitlySetVariables() {
        setEnvironmentVariables
    }

    /** Combine the copied (calling environment) variables and the explicitly set variables.
     *  Variables are not returned in an explicitly fixed order.
     */
    private List<AnyEscapableString> getEnvironmentExportParameters() {
        (exportedEnvironmentVariables + explicitlySetVariables).
            collect { Map.Entry<String, AnyEscapableString> kv ->
                    [u("--env"), u(kv.key) + e("=") + kv.value]
                }.
                flatten() as List<AnyEscapableString>
    }

    private List<AnyEscapableString> getFinalEngineArg() {
        environmentExportParameters +
        workingDirParameter +
        (engineArgs.collect { String it -> u(it) } as List<AnyEscapableString>)
    }

    /**
     * Ensure that if a bind-point is below another bind_point in the container, that the
     * high bind-point appears first in the list.
     *
     * This does not preserve the input order of paths.
     *
     * This only uses the apparent path, but does not account for symbolic links.
     */
    private static List<BindSpec> sortMounts(@NotNull List<BindSpec> specs) {
        specs.sort().unique()
    }

    /**
     *  Ensure that the same target path is not used multiple times for different host paths.
     *  If a directory is listed multiple times, return it only once, with the more relaxed
     *  (RW > RO) access mode.
     *
     *  This only uses the apparent paths, but does not account for symbolic links.
     */
    private static List<BindSpec> deduplicateAndCheckBindSpecs(@NotNull List<BindSpec> specs) {
        LinkedHashMap<Path, BindSpec> containerPathIndex = [:]
        for (bindSpec in specs) {
            Optional<BindSpec> previousO =
                    Optional.ofNullable(containerPathIndex.get(bindSpec.containerPath))
            if (previousO.present) {
                BindSpec previous = previousO.get()
                if (previous.hostPath != bindSpec.hostPath) {
                    throw new IllegalArgumentException(
                            "Cannot bind different host paths to same container path: " +
                            "container = $bindSpec.containerPath, " +
                            "host = " + [previous.hostPath, bindSpec.hostPath].toString())
                } else if (previous.mode < bindSpec.mode) {
                    containerPathIndex[previous.containerPath] = bindSpec
                }
            } else {
                containerPathIndex[bindSpec.containerPath] = bindSpec
            }
        }
        containerPathIndex.values().toList()
    }

    /**
     *  Ensure that target paths mounted below another target paths are listed later in the list.
     *  It is possible to mount a RW path below an RO path.
     */
    private static List<BindSpec> prepareBindSpecs(@NotNull List<BindSpec> specs) {
        sortMounts(deduplicateAndCheckBindSpecs(specs))
    }

    /**
     * Translate the bind specifications into command-line parameters for apptainer/singularity.
     * @return
     */
    private List<AnyEscapableString> getBindOptions() {
        if (bindSpecifications.size() > 0) {
            prepareBindSpecs(bindSpecifications).
                    collect { [u("-B"), e(it.toBindOption())] }.
                    flatten() as List<AnyEscapableString>
        } else {
            []
        }
    }

    /** Compose the information into a valid apptainer/singularity call. This fails, if the
     *  imageId is obviously invalid (null or zero length; no more checks).
     *
     * @param imageId      Optional imageId. Fall back to the instance-level configured image,
     *                     if it exists.
     * @return
     */
    Command build(@Nullable String imageId) {
        String _imageId = this.imageId
        if (imageId != null) {
            _imageId = imageId
        }
        Preconditions.checkArgument(_imageId != null && _imageId.length() > 0,
                                    "imageId cannot be null and must not be empty")
        new Command(
                new Executable(apptainerExecutable),
                ([u("exec")] as List<AnyEscapableString>) +
                bindOptions +
                finalEngineArg +
                ([e(_imageId)] as List<AnyEscapableString>))
    }

}
