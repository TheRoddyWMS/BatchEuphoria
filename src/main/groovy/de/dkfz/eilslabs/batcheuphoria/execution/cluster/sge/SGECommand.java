///*
// * Copyright (c) 2016 eilslabs.
// *
// * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
// */
//
//package de.dkfz.eilslabs.batcheuphoria.execution.cluster.sge;
//
//import de.dkfz.eilslabs.batcheuphoria.execution.ExecutionService;
//import de.dkfz.eilslabs.batcheuphoria.execution.cluster.pbs.PBSCommand;
//import de.dkfz.eilslabs.batcheuphoria.jobs.Job;
//import de.dkfz.eilslabs.batcheuphoria.jobs.ProcessingCommands;
//
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by michael on 20.05.14.
// */
//public class SGECommand extends PBSCommand {
//    /**
//     * @param job
//     * @param executionService
//     * @param id
//     * @param processingCommands
//     * @param parameters
//     * @param arrayIndices
//     * @param dependencyIDs
//     * @param command
//     */
//    public SGECommand(Job job, ExecutionService executionService, String id, List<ProcessingCommands> processingCommands, Map<String, String> parameters, List<String> arrayIndices, List<String> dependencyIDs, String command) {
//        super(job, id, processingCommands, parameters, arrayIndices, dependencyIDs, command,null, null);
//    }
//
//    @Override
//    public String getJoinLogParameter() {
//        return " -j y";
//    }
//
//    @Override
//    public String getEmailParameter(String address) {
//        return " -M " + address;
//    }
//
//    @Override
//    public String getGroupListString(String groupList) {
//        return " ";
//    }
//
//    @Override
//    public String getUmaskString(String umask) {
//        return " ";
//    }
//
//    @Override
//    public String getDependencyParameterName() {
//        return "-hold_jid";
//    }
//
//    @Override
//    public String getDependencyTypesSeparator() {
//        return " ";
//    }
//
//    @Override
//    public String getDependencyOptionSeparator() {
//        return " ";
//    }
//
//    @Override
//    public String getDependencyIDSeparator() {
//        return ",";
//    }
//
//    @Override
//    public String getArrayDependencyParameterName() {
//        return "-hold_jid_ad";
//    }
//
//    @Override
//    protected String getAdditionalCommandParameters() {
//        return " -S /bin/bash ";
//    }
//
//    @Override
//    protected String getDependsSuperParameter() {
//        return " ";
//    }
//}
