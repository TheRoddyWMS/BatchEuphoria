package de.dkfz.roddy.execution.jobs

import spock.lang.Specification

class ExtendedJobInfoTest extends Specification {
    def "test @EqualsAndHashCode annotation with differing base class"() {
        when:
        def g1 = new ExtendedJobInfo(new BEJobID("1000"))
        def g2 = new ExtendedJobInfo(new BEJobID("1001"))

        then:
        g1 != g2
    }
}
