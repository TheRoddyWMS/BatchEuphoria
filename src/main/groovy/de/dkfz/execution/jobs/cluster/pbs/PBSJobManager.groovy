/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.execution.jobs.cluster.pbs

import de.dkfz.execution.jobs.cluster.ClusterJobManager

/**
 * A job submission implementation for standard PBS systems.
 *
 * @author michael
 */
@groovy.transform.CompileStatic
public abstract class PBSJobManager extends ClusterJobManager<PBSCommand> {


}
