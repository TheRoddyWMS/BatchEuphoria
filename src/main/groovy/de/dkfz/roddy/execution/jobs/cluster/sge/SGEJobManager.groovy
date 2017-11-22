/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.sge

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobManagerCreationParameters
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSJobManager
import de.dkfz.roddy.tools.BufferUnit
import groovy.transform.CompileStatic
import sun.reflect.generics.reflectiveObjects.NotImplementedException

/**
 * Created by michael on 20.05.14.
 */
@CompileStatic
class SGEJobManager extends PBSJobManager {
    SGEJobManager(BEExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms)
    }

    SGECommand createCommand(BEJob job) {
        return new SGECommand(this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id, job.tool?.getAbsolutePath() ?: job.getToolScript(), null)
    }

    @Override
    String getResourceOptionsPrefix() {
        return "SGEResourceOptions_"
    }

    @Override
    ProcessingParameters convertResourceSet(BEJob job, ResourceSet resourceSet) {
        LinkedHashMultimap<String, String> resourceParameters = LinkedHashMultimap.create()
//        if (resourceSet.isQueueSet()) {
//            resourceParameters.put("-q", resourceSet.getQueue())
//        }
        if (resourceSet.isMemSet()) {
            String memo = resourceSet.getMem().toString(BufferUnit.M)
            resourceParameters.put("-M", memo.substring(0, memo.toString().length() - 1))
        }
//        if (resourceSet.isWalltimeSet()) {
//            resourceParameters.put("-W", durationToLSFWallTime(resourceSet.getWalltimeAsDuration()))
//        }
//        if (resourceSet.isCoresSet() || resourceSet.isNodesSet()) {
//            int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
//            resourceParameters.put("-n", nodes.toString())
//        }

        StringBuilder sb = new StringBuilder()
        sb.append(" -V") //TODO Think if default SGE options should go somewhere else?
        if (resourceSet.isMemSet()) {
            resourceParameters.put('-l', 's_data=' + resourceSet.getMem().toString(BufferUnit.G) + 'g')
        }
        if (resourceSet.isStorageSet()) {
        }
        return new ProcessingParameters(resourceParameters)
    }

    @Override
    ProcessingParameters extractProcessingParametersFromToolScript(File file) {
        return null
    }

    @Override
    String getStringForQueuedJob() {
        return "qw"
    }

    @Override
    String getStringForJobOnHold() {
        return "hqw"
    }

    @Override
    String getStringForRunningJob() {
        return "r"
    }

    @Override
    protected int getPositionOfJobID() {
        return 0
    }

    @Override
    protected int getPositionOfJobState() {
        return 4
    }

    @Override
    String parseJobID(String commandOutput) {
        if (!commandOutput.startsWith("Your job"))
            return null
        String id = commandOutput.split(StringConstants.SPLIT_WHITESPACE)[2]
        return id
    }

    protected List<String> getTestQstat() {
        return Arrays.asList(
                "job - ID prior name user jobState submit / start at queue slots ja -task - ID",
                "---------------------------------------------------------------------------------------------------------------- -",
                "   1187 0.75000 r140710_09 seqware r 07 / 10 / 2014 09:51:55 main.q @worker3 1",
                "   1188 0.41406 r140710_09 seqware r 07 / 10 / 2014 09:51:40 main.q @worker1 1",
                "   1190 0.25000 r140710_09 seqware r 07 / 10 / 2014 09:51:55 main.q @worker2 1",
                "   1189 0.00000 r140710_09 seqware hqw 07 / 10 / 2014 09:51:27 1",
                "   1191 0.00000 r140710_09 seqware hqw 07 / 10 / 2014 09:51:48 1",
                "   1192 0.00000 r140710_09 seqware hqw 07 / 10 / 2014 09:51:48 1")
    }
}
