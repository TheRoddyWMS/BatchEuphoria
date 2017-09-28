/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.sge;

import org.junit.Test;

/**
 */
public class SGEJobManagerTest {

    @Test
    public void testConvertToolEntryToPBSCommandParameters() {
        // Roddy specific tests!
//        ResourceSet rset1 = new ResourceSet(ResourceSetSize.l, new BufferValue(1, BufferUnit.G), 2, 1, new TimeUnit("h"), null, null, null);
//        ResourceSet rset2 = new ResourceSet(ResourceSetSize.l, null, null, 1, new TimeUnit("h"), null, null, null);
//        ResourceSet rset3 = new ResourceSet(ResourceSetSize.l, new BufferValue(1, BufferUnit.G), 2, null, null, null, null, null);
//
//        Configuration cfg = new Configuration(new InformationalConfigurationContent(null, Configuration.ConfigurationType.OTHER, "test", "", "", null, "", ResourceSetSize.l, null, null, null, null));
//
//        BatchEuphoriaJobManager cFactory = new SGEJobManager(false);
//        PBSResourceProcessingParameters test = (PBSResourceProcessingParameters) cFactory.convertResourceSet(cfg, rset1);
//        assert test.getProcessingCommandString().trim().equals("-V -l s_data=1024M");
//
//        test = (PBSResourceProcessingParameters) cFactory.convertResourceSet(cfg, rset2);
//        assert test.getProcessingCommandString().equals(" -V");
//
//        test = (PBSResourceProcessingParameters) cFactory.convertResourceSet(cfg, rset3);
//        assert test.getProcessingCommandString().equals(" -V -l s_data=1024M");
    }


}
