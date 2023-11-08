package de.dkfz.roddy.execution


import com.google.common.base.Preconditions
import groovy.transform.CompileStatic
import org.jetbrains.annotations.NotNull

import javax.annotation.Nullable
import java.nio.file.Path
import java.nio.file.Paths

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
        Preconditions.checkArgument(hostPath != null)
        this.hostPath = hostPath
        if (containerPath == null) {
            this.containerPath = hostPath
        } else {
            this.containerPath = containerPath
        }
        Preconditions.checkArgument(opts != null)
        this.mode = opts
    }

    String toBindOption() {
        if (containerPath == hostPath) "$hostPath:$mode"
        else "$hostPath:$containerPath:$mode"
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


@CompileStatic
class ApptainerCommandBuilder {

    private @NotNull List<BindSpec> bindSpecifications

    private @NotNull Path apptainerExecutable = Paths.get("apptainer")

    private @NotNull List<String> engineArgs

    private String imageId

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
     * @param bindSpecifications     Specification of the directories to bind into the container.
     * @param engineArgs             Further CLI arguments for apptainer/singularity.
     * @param apptainerExecutable    Path to executable. Defaults to "apptainer" (relative path)
     */
    ApptainerCommandBuilder(@NotNull List<BindSpec> bindSpecifications,
                            @NotNull List<String> engineArgs = ["run", "--contain"],
                            @NotNull Path apptainerExecutable = Paths.get("apptainer"),
                            @Nullable String imageId = null) {
        this.bindSpecifications = bindSpecifications
        this.engineArgs = engineArgs
        this.apptainerExecutable = apptainerExecutable
        Preconditions.checkArgument(imageId == null || imageId.length() > 0)
        this.imageId = imageId
    }

    Optional<String> getImageId() {
        Optional.ofNullable(this.imageId)
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
     * @param specs
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
    private List<String> getBindOptions() {
        if (bindSpecifications.size() > 0) {
            prepareBindSpecs(bindSpecifications).
                    collect { ["-B", it.toBindOption()] }.
                    flatten() as List<String>
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
    Command build(String imageId) {
        String _imageId = this.imageId
        if (imageId != null) {
            _imageId = imageId
        }
        Preconditions.checkArgument(_imageId != null && _imageId.length() > 0)
        new Command(
                new Executable(apptainerExecutable),
                ["exec"] + bindOptions + engineArgs + [_imageId])
    }

}
