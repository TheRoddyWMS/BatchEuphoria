/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs


import de.dkfz.roddy.core.InfoObject
import groovy.transform.CompileStatic

import static de.dkfz.roddy.tools.EscapableString.*

/**
 * Created by heinold on 27.02.17.
 */
@CompileStatic
class FakeBEJob extends BEJob {

    FakeBEJob(BEFakeJobID jobID) {
        super(jobID, null, u('Fakejob'))
    }

    FakeBEJob() {
        super(null, null, u('Fakejob'))
    }

    FakeBEJob(InfoObject context) {
        this()
    }

}
