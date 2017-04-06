/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs


import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.TimeUnit

import java.io.File
import java.util.List
import java.util.Map

/**
 * Created by michael on 06.02.15.
 */
class GenericJobInfo {

    String jobName
    File tool
    String id
    Map<String, String> parameters
    List<String> parentJobIDs
    TimeUnit walltime
    int cpus
    int nodes
    int memory
    BufferUnit memoryBufferUnit
    String queue
    String otherSettings

    GenericJobInfo(String jobName, File tool, String id, Map<String, String> parameters, List<String> parentJobIDs) {
        this.jobName = jobName
        this.tool = tool
        this.id = id
        this.parameters = parameters
        this.parentJobIDs = parentJobIDs
    }

    @Override
    String toString() {
        return "GenericJobInfo{" +
                "jobName='" + jobName + '\'' +
                ", tool='" + tool + '\'' +
                ", id='" + id + '\'' +
                ", parameters=" + parameters +
                ", parentJobIDs=" + parentJobIDs +
                '}'
    }
}
