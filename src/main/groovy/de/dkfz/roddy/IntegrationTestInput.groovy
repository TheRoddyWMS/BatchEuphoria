/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy

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
