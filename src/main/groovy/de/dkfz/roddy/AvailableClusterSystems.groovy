/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy

import de.dkfz.roddy.execution.jobs.cluster.lsf.LSFJobManager
import de.dkfz.roddy.execution.jobs.cluster.lsf.rest.LSFRestJobManager
import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSJobManager
import de.dkfz.roddy.execution.jobs.direct.synchronousexecution.DirectSynchronousExecutionJobManager
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import groovy.transform.CompileStatic

/**
 * A list of available cluster systems.
 * testable means that the tests are basically implemented
 * Created by heinold on 27.03.17.
 */
@CompileStatic
enum AvailableClusterSystems {
    direct(DirectSynchronousExecutionJobManager.class), pbs(PBSJobManager.class), sge("de.dkfz.eilslabs.batcheuphoria.execution.cluster.sge.SGEJobManager"), slurm("de.dkfz.eilslabs.batcheuphoria.execution.cluster.slurm.SlurmJobManager"), lsf(LSFJobManager.class), lsfrest(LSFRestJobManager.class)

    final String className

    AvailableClusterSystems(String className) {
        this.className = className
    }

    AvailableClusterSystems(Class cls) {
        this.className = cls.name
    }

    Class<BatchEuphoriaJobManager> loadClass() {
        return getClass().getClassLoader().loadClass(className) as Class<BatchEuphoriaJobManager>
    }
}
