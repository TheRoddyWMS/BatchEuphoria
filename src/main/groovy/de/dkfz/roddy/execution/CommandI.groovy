/*
 * Copyright (c) 2023
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution

import de.dkfz.roddy.tools.BashUtils
import groovy.transform.CompileStatic
import org.apache.commons.lang3.RandomStringUtils

import javax.annotation.Nonnull
import javax.annotation.Nullable
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
    abstract List<String> getCommand(boolean absolutePath)

}

/** A self-contained command that does not take any command-line parameters. This is mostly
 *  used for Roddy tools, which are entirely configured via environment variables.
 */
@CompileStatic
class Executable extends CommandReferenceI {

    private Path path

    private @Nullable String md5

    Executable(@Nonnull Path path,
               @Nullable String md5 = null) {
        this.path = path
        this.md5 = md5
    }

    @Override
    Path getExecutablePath() {
        this.path
    }

    @Override
    List<String> getCommand(boolean absolutePath = false) {
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
class Command extends CommandReferenceI {

    private Executable executable

    private List<String> arguments

    Command(@Nonnull Executable executable,
            @Nonnull List<String> arguments) {
        this.executable = executable
        this.arguments = arguments.asImmutable()
    }

    Executable getExecutable() {
        executable
    }

    Path getExecutablePath() {
        executable.getExecutablePath()
    }

    List<String> getCommand(boolean absolutePath = false) {
        executable.getCommand(absolutePath) + arguments
    }

    /** Append the Code as script to the command. The result is a Code object, not a
     *  SimpleCommand, because the appending is done via a HERE document
     *
     * @param other
     * @param terminator
     * @return
     */
    Code cliAppend(@Nonnull Code other,
                   boolean absolutePath = false,
                   @Nonnull Path interpreter = Paths.get("/bin/bash"),  // Could be Command
                   @Nonnull String terminator = "batch_euphoria_" +
                           RandomStringUtils.random(10, true, true)) {
        new Code("""\
                    |${this.getCommand(absolutePath).join(" ")} <<$terminator
                    |#!${other.interpreter}
                    |${other.code}
                    |$terminator
                    |""",
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
    Command cliAppend(@Nonnull CommandReferenceI other,
                      boolean absolutePath = false,
                      boolean quote = false) {
        if (!quote) {
            new Command(
                    executable,
                    this.arguments + other.getCommand(absolutePath))
        } else {
            new Command(
                    executable,
                    this.arguments +
                            BashUtils.strongQuote(other.getCommand(absolutePath).join(" ")))
        }
    }

}


/** Take actual code to be executed. */
@CompileStatic
class Code extends CommandI {

    private String code

    private Path interpreter

    Code(@Nonnull String code,
         @Nonnull Path interpreter = Paths.get("/bin/bash")) {
        assert(code.size() > 0)
        this.code = code
        this.interpreter = interpreter
    }

    String getCode() {
        code
    }

    Path getInterpreter() {
        interpreter
    }

}
