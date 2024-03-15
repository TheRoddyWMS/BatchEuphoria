/*
 * Copyright (c) 2023
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution

import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import org.apache.commons.lang3.RandomStringUtils
import org.jetbrains.annotations.NotNull

import java.nio.file.Path
import java.nio.file.Paths

import static de.dkfz.roddy.execution.EscapableString.*


/** Types of executable code or command. These are used as arguments for BEJob. The
 *  class hierarchy represents and abstract data type (ADT).
 */
@CompileStatic
abstract class CommandI {

    abstract AnyEscapableString toEscapableString()

}

/** A command reference is opposed to the command code. Both are executable, but the command
 *  reference only represents a short name for a piece of code, usually on the filesystem.
 */
@CompileStatic
abstract class CommandReferenceI extends CommandI {

    /** The path to the executable file that will be executed. */
    abstract Path getExecutablePath()

    /** Get a list with command components, e.g. for Bash.
     *
     * @return
     */
    abstract List<AnyEscapableString> toCommandSegmentList()

    @Override
    AnyEscapableString toEscapableString() {
        join(toCommandSegmentList(), " ")
    }

}

/** A self-contained command that does not take any command-line parameters. This is mostly
 *  used for Roddy tools, which are entirely configured via environment variables.
 */
@CompileStatic
@EqualsAndHashCode(includeFields = true,
                   excludes = ["executablePath", "md5"]) // without this getMd5 will be used, but results in different Optional instances.
final class Executable extends CommandReferenceI {

    private final Path path

    /** An executable is a file, e.g. a script, for which there can be an MD5 sum. This is
     *  important in some cases (which is why it is allowed to be `null`) if a script is
     *  uploaded to the cluster.
     */
    private final String md5

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
    List<AnyEscapableString> toCommandSegmentList() {
        [u(path.toString())] as List<AnyEscapableString>
    }

    Optional<String> getMd5() {
        Optional.ofNullable(this.md5)
    }

}

/** A command is an executable with 0 or more arguments.
 */
@CompileStatic
@EqualsAndHashCode(includeFields = true)
final class Command extends CommandReferenceI {

    private final Executable executable

    private final ImmutableList<AnyEscapableString> arguments

    /** Concerning quoting arguments: Provide arguments like they should be used at the call-site.
     *
     * @param executable     The executable to be called.
     * @param arguments      The arguments to be passed to the executable.
     **/
    Command(@NotNull Executable executable,
            @NotNull List<AnyEscapableString> arguments = []) {
        Preconditions.checkArgument(executable != null)
        this.executable = executable
        Preconditions.checkArgument(arguments != null)
        arguments.forEach {
            Preconditions.checkArgument(it != null,
                    "Command.arguments must not contain null for executable: " +
                            executable.executablePath.toString())
        }
        this.arguments = ImmutableList.copyOf(arguments)
    }

    Executable getExecutable() {
        executable
    }

    Path getExecutablePath() {
        executable.getExecutablePath()
    }

    List<AnyEscapableString> toCommandSegmentList() {
        executable.toCommandSegmentList() + arguments
    }

    /** Append the Code as script to the command. The result is a Code object, not a
     *  SimpleCommand, because the appending is done via a HERE document
     *
     * @param other
     * @param interpreter        The interpreter must be Bash. The executable must be `bash`.
     * @param terminator_prefix
     * @return
     */
    @CompileDynamic
    Code cliAppend(@NotNull Code other,
                   @NotNull CommandReferenceI interpreter, // = new Executable(Paths.get("/bin/bash")),
                                                           // This gives a "Bad type on operand stack" error
                   @NotNull String terminator_prefix = "batch_euphoria_",
                   @NotNull String terminator_random =
                           RandomStringUtils.random(10, true, true)) {
        Preconditions.checkArgument(other != null)
        Preconditions.checkArgument(interpreter != null)
        Preconditions.checkArgument(interpreter.executablePath.getFileName().toString() == "bash")
        Preconditions.checkArgument(terminator_prefix != null)
        Preconditions.checkArgument(terminator_random != null)
        String terminator = terminator_prefix + "_" + terminator_random
        new Code(c([
                   this.toEscapableString() + " <<$terminator" + "\n" +
                    "#!" + other.interpreter.toEscapableString() + "\n" +
                    other.code + "\n" +
                    terminator + "\n"   // Termination with terminator + "\n" is important for Bash.
                   ]),
                 interpreter)
    }

    /** Append the other Command to the command-line of this Command. Use this to
     *  e.g. executed one command in the context of another, like e.g. `strace $other`,
     *  `singularity run $other`.
     *
     * @param other
     * @param appendAsString     Whether to quote the appended Command.
     * @return
     */
    @CompileDynamic
    Command cliAppend(@NotNull CommandReferenceI other,
                      boolean appendAsString = false) {
        Preconditions.checkArgument(other != null)
        if (!appendAsString) {
            new Command(
                    executable,
                    this.arguments +
                    other.toCommandSegmentList())
        } else {
            new Command(
                    executable,
                    (this.arguments + e(other.toEscapableString())) as List<AnyEscapableString>)
        }
    }

}


/** Take actual code to be executed. */
@CompileStatic
@EqualsAndHashCode(includeFields = true)
final class Code extends CommandI {

    /** Code will usually be a script, maybe with a shebang line. Code may or may not be provided
     *  to the job submission command (e.g. bsub) via the standard input instead of as file.
     */
    @NotNull private final AnyEscapableString code

    /** An interpreter for the code. This is bash by default, but could (probably) also be
     *  python3, perl, or whatever. It is also possible to use commands with arguments as
     *  interpreters, e.g. `/bin/bash -xe`
     */
    @NotNull private final CommandReferenceI interpreter

    Code(@NotNull AnyEscapableString code,
         @NotNull CommandReferenceI interpreter) {
        Preconditions.checkArgument(code != null)
        Preconditions.checkArgument(code.size() > 0)
        this.code = code
        Preconditions.checkArgument(interpreter != null)
        this.interpreter = interpreter
    }

    // Convenience constructors

    Code(@NotNull String code,
        @NotNull Path interpreter = Paths.get("/bin/bash")) {
        this(u(code), new Executable(interpreter))
    }

    Code(@NotNull AnyEscapableString code,
         @NotNull Path interpreter = Paths.get("/bin/bash")) {
        this(code, new Executable(interpreter))
    }

    Code(@NotNull String code,
         @NotNull CommandReferenceI interpreter) {
        this(u(code), interpreter)
    }

    AnyEscapableString getCode() {
        code
    }

    CommandReferenceI getInterpreter() {
        interpreter
    }

    /** Create a little script. Note that the escapable string does *not* have automatically an
     *  newline appended, because it depends on the client context whether this is useful. For
     *  instance, a newline may be unnecessary in many cases, but for a HERE document the terminator
     *  must be followed by newline. */
    AnyEscapableString toEscapableString(boolean terminate) {
        AnyEscapableString result = u("#!") + interpreter.toEscapableString() + "\n" + code
        if (terminate) {
            result += "\n"
        }
        return result
    }

    @Override
    AnyEscapableString toEscapableString() {
        toEscapableString(false)
    }

}
