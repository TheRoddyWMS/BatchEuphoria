/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria

import de.dkfz.eilslabs.batcheuphoria.execution.cluster.pbs.PBSJobManager
import de.dkfz.eilslabs.batcheuphoria.execution.direct.synchronousexecution.DirectSynchronousExecutionJobManager
import de.dkfz.eilslabs.batcheuphoria.jobs.JobManager
import groovy.transform.CompileStatic

/**
 *
 * Created by kaercher on 04.04.17.
 */
@CompileStatic
class IntegrationTestInput {

    AvailableClusterSystems clusterSystem

    String account
    String server

    String restAccount
    String restServer

}
