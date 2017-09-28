/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.core.InfoObject
import groovy.transform.CompileStatic

/**
 * Created by heinold on 27.02.17.
 */
@CompileStatic
class FakeBEJob extends BEJob {

    FakeBEJob(BEFakeJobID jobID) {
        super(jobID, 'Fakejob', null, null, "", null, [], [:], null)
    }

    FakeBEJob() {
        super(null, 'Fakejob', null, null, "", null, [], [:], null)
    }

    FakeBEJob(InfoObject context) {
        this()
    }

}
