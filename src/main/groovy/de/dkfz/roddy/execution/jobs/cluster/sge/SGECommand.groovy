/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.sge

import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSCommand
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.ProcessingCommands
import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSJobManager
import groovy.transform.CompileStatic

import java.io.File
import java.util.List
import java.util.Map

/**
 * Created by michael on 20.05.14.
 */
@CompileStatic
class SGECommand extends PBSCommand {

    SGECommand(PBSJobManager parentManager, BEJob job, String id, List<ProcessingCommands> processingCommands, Map<String, String> parameters, Map<String, Object> tags, List<String> arrayIndices, List<String> dependencyIDs, String command, File loggingDirectory) {
        super(parentManager, job, id, processingCommands, parameters, tags, arrayIndices, dependencyIDs, command, loggingDirectory)
    }

    @Override
    String getJoinLogParameter() {
        return " -j y"
    }

    @Override
    String getEmailParameter(String address) {
        return " -M " + address
    }

    @Override
    String getGroupListString(String groupList) {
        return " "
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
    String getDependencyTypesSeparator() {
        return " "
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
    String getArrayDependencyParameterName() {
        return "-hold_jid_ad"
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
