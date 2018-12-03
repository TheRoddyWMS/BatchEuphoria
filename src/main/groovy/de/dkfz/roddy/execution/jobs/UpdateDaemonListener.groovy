/*
 * Copyright (c) 2018 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://github.com/DKFZ-ODCF/COWorkflowsBasePlugin/LICENSE.txt).
 */
package de.dkfz.roddy.execution.jobs

import groovy.transform.CompileStatic

/**
 * Listener interface for ended jobs, if the update daemon is active.
 * Use JobManager.addUpdateDaemonListener to add a listener.
 */
@CompileStatic
interface UpdateDaemonListener {
    void jobEnded(BEJob job, JobState state)
}
