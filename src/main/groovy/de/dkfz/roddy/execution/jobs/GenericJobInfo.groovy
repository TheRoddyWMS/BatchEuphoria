/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.config.ResourceSet
import groovy.transform.CompileStatic
import groovy.transform.ToString

import java.time.Duration
import java.time.LocalDateTime

/**
 * Created by michael on 06.02.15.
 */
@CompileStatic
@ToString(includeNames=true)
class GenericJobInfo {

    ResourceSet askedResources
    ResourceSet usedResources
    String jobName
    File tool
    @Deprecated String id

    LocalDateTime submitTime;
    LocalDateTime startTime;
    LocalDateTime endTime;
    LocalDateTime eligibleTime; // when all conditions like job dependencies full filled, it is qu

    List<String> executionHosts;
    String submissionHost;
    String priority;

    File logFile
    File errorLogFile
    File inputFile

    String user;
    String userGroup;
    String resourceReq; // resource requirements
    Integer startCount

    String account
    String server
    String umask

    Map<String, String> parameters
    List<String> parentJobIDs
    String otherSettings
    JobState jobState
    String userTime; //user time used
    String systemTime; //system time used
    String pendReason;
    String execHome;
    String execUserName;
    List<String> pidStr;
    String pgidStr; // Currently active process group ID in a job.
    Integer exitCode; // UNIX exit status of the job
    String jobGroup;
    String description;
    String execCwd; //Executed current working directory
    String askedHostsStr;
    String cwd; //Current working directory
    String projectName;

    Duration cpuTime; //Cumulative total CPU time in seconds of all processes in a job
    Duration runTime; //Time in seconds that the job has been in the run state
    Duration timeUserSuspState; //Suspended by its owner or the LSF administrator after being dispatched
    Duration timePendState; //Waiting in a queue for scheduling and dispatch
    Duration timePendSuspState; // Suspended by its owner or the LSF administrator while in PEND state
    Duration timeSystemSuspState; //Suspended by the LSF system after being dispatched
    Duration timeUnknownState;
    LocalDateTime timeOfCalculation;


    GenericJobInfo(String jobName, File tool, String id, Map<String, String> parameters, List<String> parentJobIDs) {
        this.jobName = jobName
        this.tool = tool
        this.id = id
        this.parameters = parameters
        this.parentJobIDs = parentJobIDs
    }
}
