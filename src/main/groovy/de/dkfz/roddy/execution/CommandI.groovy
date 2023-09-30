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


@CompileStatic
interface CommandI {}


@CompileStatic
class ScriptCommand implements CommandI {

    private Path script

    private @Nullable String md5

    ScriptCommand(@Nonnull Path script,
                  @Nullable String md5 = null) {
        this.script = script
        this.md5 = md5
    }

    Path getScript() {
        this.script
    }

    Optional<String> getMd5() {
        Optional.ofNullable(this.md5)
    }

    Command toCommand() {
        new Command([this.script.toString()])
    }

}


@CompileStatic
class Command implements CommandI {

    private List<String> command

    Command(@Nonnull List<String> command) {
        assert(command.size() > 0)
        this.command = command.asImmutable()
    }

    List<String> getCommand() {
        this.command
    }

    /** Append the Code as script to the command. The result is a Code object, not a
     *  SimpleCommand, because the appending is done via a HERE document
     *
     * @param other
     * @param terminator
     * @return
     */
    Code cliAppend(@Nonnull Code other,
                   @Nonnull Path interpreter = Paths.get("/bin/bash"),  // Could be Command
                   @Nonnull String terminator = "batch_euphoria_" +
                           RandomStringUtils.random(10, true, true)) {
        new Code("""\
                    |${this.command.join(" ")} <<$terminator
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
    Command cliAppend(@Nonnull Command other, quote=false) {
        if (!quote) {
            new Command(this.command + other.command)
        } else {
            new Command(this.command + BashUtils.strongQuote(other.command.join(" ")))
        }
    }

}


/** Take actual code to be executed. */
@CompileStatic
class Code implements CommandI {

    final String code

    final Path interpreter

    Code(@Nonnull String code,
         @Nonnull Path interpreter = Paths.get("/bin/bash")) {
        assert(code.size() > 0)
        this.code = code
        this.interpreter = interpreter
    }

}
