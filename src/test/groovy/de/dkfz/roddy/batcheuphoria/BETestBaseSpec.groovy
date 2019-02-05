/*
 * Copyright (c) 2019 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/BatchEuphoria/LICENSE.txt).
 */

package de.dkfz.roddy.batcheuphoria


import spock.lang.Specification

/**
 * Base Spock spec for BE Spock specs
 * Contains commonly used test methods
 */
class BETestBaseSpec extends Specification {

    static final File getResourceFile(Class _class, String file) {
        String subDir = _class.package.name.replace(".", "/")
        new File("src/test/resources/${subDir}/", file)
    }

    def "test getResourceFile"() {
        when:
        File resourceFile = getResourceFile(BETestBaseSpec, "getResourceFileTest.txt")
        String text = resourceFile.text

        then:
        resourceFile.exists()
        text == "abc"
    }
}
