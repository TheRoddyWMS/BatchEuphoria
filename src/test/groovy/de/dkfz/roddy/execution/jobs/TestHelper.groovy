package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult

import java.time.Duration

class TestHelper {

    static BEExecutionService makeExecutionService() {
        return new BEExecutionService() {
            @Override
            ExecutionResult execute(Command command) {
                return null
            }

            @Override
            ExecutionResult execute(Command command, boolean waitFor) {
                return null
            }

            @Override
            ExecutionResult execute(Command command, Duration timeout) {
                return null
            }

            @Override
            ExecutionResult execute(Command command, boolean waitFor, Duration timeout) {
                return null
            }

            @Override
            ExecutionResult execute(String command) {
                return null
            }

            @Override
            ExecutionResult execute(String command, boolean waitFor) {
                return null
            }

            @Override
            ExecutionResult execute(String command, Duration timeout) {
                return null
            }

            @Override
            ExecutionResult execute(String command, boolean waitFor, Duration timeout) {
                return null
            }

            @Override
            boolean isAvailable() {
                return false
            }

            @Override
            File queryWorkingDirectory() {
                return null
            }
        }
    }

}
