/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import groovy.transform.CompileStatic;
import org.junit.Test;

/**
 */
@CompileStatic
public class PBSJobManagerTest {

    @Test
    public void testConvertToolEntryToPBSCommandParameters() {
        // Roddy specific tests!
//        ResourceSet rset1 = new ResourceSet(ResourceSetSize.l, new BufferValue(1, BufferUnit.G), 2, 1, new TimeUnit("h"), null, null, null);
//        ResourceSet rset2 = new ResourceSet(ResourceSetSize.l, null, null, 1, new TimeUnit("h"), null, null, null);
//        ResourceSet rset3 = new ResourceSet(ResourceSetSize.l, new BufferValue(1, BufferUnit.G), 2, null, null, null, null, null);
//
//        Configuration cfg = new Configuration(new InformationalConfigurationContent(null, Configuration.ConfigurationType.OTHER, "test", "", "", null, "", ResourceSetSize.l, null, null, null, null));
//
//        BatchEuphoriaJobManager cFactory = new PBSJobManager(false);
//        PBSResourceProcessingCommand test = (PBSResourceProcessingCommand) cFactory.convertResourceSet(cfg, rset1);
//        assert test.getProcessingString().trim().equals("-l mem=1024M -l nodes=1:ppn=2 -l walltime=00:01:00:00");
//
//        test = (PBSResourceProcessingCommand) cFactory.convertResourceSet(cfg, rset2);
//        assert test.getProcessingString().equals(" -l nodes=1:ppn=1 -l walltime=00:01:00:00");
//
//        test = (PBSResourceProcessingCommand) cFactory.convertResourceSet(cfg, rset3);
//        assert test.getProcessingString().equals(" -l mem=1024M -l nodes=1:ppn=2");
    }
}
