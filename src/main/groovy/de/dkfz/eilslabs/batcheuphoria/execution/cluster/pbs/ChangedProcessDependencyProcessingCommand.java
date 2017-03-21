/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.execution.cluster.pbs;

import de.dkfz.eilslabs.batcheuphoria.jobs.ProcessingCommands;

/**
 * Change a jobs dependency from afterok (default) to something different.
 */
public class ChangedProcessDependencyProcessingCommand extends ProcessingCommands {
    private final ProcessDependency processDependency;

    public enum ProcessDependency {
        after,
        afterok,
        afterfail,
        before,
        beforeok
    }

    public ChangedProcessDependencyProcessingCommand(ProcessDependency processDependency) {
        this.processDependency = processDependency;
    }

    public ProcessDependency getProcessDependency() {
        return processDependency;
    }
}
