/*
 * Copyright (c) 2023
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution

import com.google.common.base.Preconditions
import de.dkfz.roddy.tools.shell.bash.Service as BashService
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import org.apache.commons.lang3.RandomStringUtils
import org.jetbrains.annotations.NotNull

import java.nio.file.Path
import java.nio.file.Paths

/** Types of executable code or command. These are used as arguments for BEJob. The
 *  class hierarchy represents and abstract data type (ADT).
 */
@CompileStatic
abstract class CommandI {}

/** A command reference is opposed to the command code. Both are executable, but the command
 *  reference only represents a short name for a piece of code, usually on the filesystem.
 */
@CompileStatic
abstract class CommandReferenceI extends CommandI {

    /** The path to the executable file that will be executed. */
    abstract Path getExecutablePath()

    /** Get a list with command components, e.g. for Bash.
     *
     * @param absolutePath  Whether to call toAbsolutePath() on the executable's path.
     * @return
     */
    abstract List<String> toCommandSegmentList(boolean absolutePath)

}

/** A self-contained command that does not take any command-line parameters. This is mostly
 *  used for Roddy tools, which are entirely configured via environment variables.
 */
@CompileStatic
@EqualsAndHashCode
final class Executable extends CommandReferenceI {

    private Path path

    /** An executable is a file, e.g. a script, for which there can be an MD5 sum. This is
     *  important in some cases (which is why it is allowed to be `null`) if a script is
     *  uploaded to the cluster.
     */
    private String md5

    Executable(@NotNull Path path,
               String md5 = null) {
        Preconditions.checkArgument(path != null)
        this.path = path
        Preconditions.checkArgument(md5 == null || md5.size() == 32)
        this.md5 = md5
    }

    @Override
    Path getExecutablePath() {
        this.path
    }

    @Override
    List<String> toCommandSegmentList(boolean absolutePath = false) {
        if (absolutePath) {
            [path.toAbsolutePath().toString()]
        } else {
            [path.toString()]
        }
    }

    Optional<String> getMd5() {
        Optional.ofNullable(this.md5)
    }

}

/** A command is an executable with 0 or more arguments.
 */
@CompileStatic
@EqualsAndHashCode
final class Command extends CommandReferenceI {

    private Executable executable

    private List<String> arguments

    Command(@NotNull Executable executable,
            @NotNull List<String> arguments = []) {
        this.executable = executable
        arguments.forEach {
            Preconditions.checkArgument(it != null,
                    "Command.arguments must not contain null for executable: " +
                            executable.executablePath.toString())
        }
        this.arguments = arguments.asImmutable()
    }

    Executable getExecutable() {
        executable
    }

    Path getExecutablePath() {
        executable.getExecutablePath()
    }

    List<String> toCommandSegmentList(boolean absolutePath = false) {
        executable.toCommandSegmentList(absolutePath) + arguments
    }

    /** Append the Code as script to the command. The result is a Code object, not a
     *  SimpleCommand, because the appending is done via a HERE document
     *
     * @param other
     * @param terminator_prefix
     * @return
     */
    @CompileDynamic
    Code cliAppend(@NotNull Code other,
                   boolean absolutePath = false,
                   @NotNull Path interpreter = Paths.get("/bin/bash"),  // Could be Command
                   @NotNull String terminator_prefix = "batch_euphoria_",
                   @NotNull String terminator_random =
                           RandomStringUtils.random(10, true, true)) {
        String terminator = terminator_prefix + "_" + terminator_random
        new Code("""\
                    |${this.toCommandSegmentList(absolutePath).join(" ")} <<$terminator
                    |#!${other.interpreter}
                    |${other.code}
                    |$terminator
                    |""".stripMargin(),
                 interpreter)
    }

    /** Append the other Command to the command-line of this Command. Use this to
     *  e.g. executed one command in the context of another, like e.g. `strace $other`,
     *  `singularity run $other`.
     *
     * @param other
     * @param quote     Whether to quote the appended Command.
     * @return
     */
    @CompileDynamic
    Command cliAppend(@NotNull CommandReferenceI other,
                      boolean absolutePath = false,
                      boolean quote = false) {
        Preconditions.checkArgument(other != null)
        if (!quote) {
            new Command(
                    executable,
                    this.arguments + other.toCommandSegmentList(absolutePath))
        } else {
            new Command(
                    executable,
                    this.arguments +
                    BashService.escape(other.toCommandSegmentList(absolutePath).join(" ")))
        }
    }

}


/** Take actual code to be executed. */
@CompileStatic
@EqualsAndHashCode
final class Code extends CommandI {

    /** Code will usually be a script, maybe with a shebang line. Code may or may not be provided
     *  to the job submission command (e.g. bsub) via the standard input instead of as file.
     */
    @NotNull private String code

    /** An interpreter for the code. This can is bash by default, but could (probably) also be
     *  python3, perl, or whatever.
     */
    @NotNull private Path interpreter

    Code(@NotNull String code,
         @NotNull Path interpreter = Paths.get("/bin/bash")) {
        Preconditions.checkArgument(code != null)
        Preconditions.checkArgument(code.size() > 0)
        this.code = code
        Preconditions.checkArgument(interpreter != null)
        this.interpreter = interpreter
    }

    String getCode() {
        code
    }

    Path getInterpreter() {
        interpreter
    }

}