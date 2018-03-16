/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.sge

import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSCommand
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSJobManager
import groovy.transform.CompileStatic

/**
 * Created by michael on 20.05.14.
 */
@CompileStatic
class SGECommand extends PBSCommand {

    SGECommand(PBSJobManager parentManager, BEJob job, String id, List<ProcessingParameters> processingCommands, Map<String, String> parameters, List<String> dependencyIDs, String command, File loggingDirectory) {
        super(parentManager, job, id, processingCommands, parameters, dependencyIDs, command)
    }

    @Override
    protected String getJoinLogParameter() {
        return "-j y"
    }

    @Override
    protected String getGroupListParameter(String groupList) {
        return ""
    }

    @Override
    String getUmaskString(String umask) {
        return " "
    }

    @Override
    String getDependencyParameterName() {
        return "-hold_jid"
    }

    @Override
    String getDependencyOptionSeparator() {
        return " "
    }

    @Override
    String getDependencyIDSeparator() {
        return ","
    }

    @Override
    protected String getAdditionalCommandParameters() {
        return " -S /bin/bash "
    }

    @Override
    protected String getDependsSuperParameter() {
        return " "
    }
}
