/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria

import groovy.transform.CompileStatic

/**
 * A list of available cluster systems.
 * testable means that the tests are basically implemented
 * Created by heinold on 27.03.17.
 */
@CompileStatic
enum AvailableClusterSystems {
    direct, pbs, sge, slurm, lsf
}
