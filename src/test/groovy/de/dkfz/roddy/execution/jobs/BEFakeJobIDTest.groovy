package de.dkfz.roddy.execution.jobs;

import org.junit.Test;

class BEFakeJobIDTest {

    @Test
    void testMatchingFakeJobIDs() {
        BEFakeJobID.FakeJobReason.values().each { reason ->
            assert new BEFakeJobID(reason).isFakeJobID()
        }
    }

}