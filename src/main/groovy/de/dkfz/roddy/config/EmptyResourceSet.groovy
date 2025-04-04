/*
 * Copyright (c) 2023 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.config


import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic

@CompileStatic
class EmptyResourceSet extends ResourceSet {
    EmptyResourceSet() {
        super(null,
              null,
              null,
              null,
              null as TimeUnit,
              null,
              null,
              null)
    }
}
