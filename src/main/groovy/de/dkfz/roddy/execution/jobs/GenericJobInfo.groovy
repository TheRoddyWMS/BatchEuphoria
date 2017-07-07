/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic

import java.time.Duration
import java.time.LocalDateTime

/**
 * Created by michael on 06.02.15.
 */
@CompileStatic
class GenericJobInfo {

    ResourceSet askedResources
    ResourceSet usedResources
    String jobName
    File tool
    String id

    LocalDateTime subTime;
    LocalDateTime startTime;
    LocalDateTime endTime;

    String exHosts; // execution hosts
    String subHost; //submission host
    String priority;

    String outFile;
    String inFile;
    String errorFile

    String user;
    String userGroup;
    String resReq; // resource requirements
    Duration cpuTime; //Cumulative total CPU time in seconds of all processes in a job
    Duration runTime; //Time in seconds that the job has been in the run state

    String server
    String umask

    Map<String, String> parameters
    List<String> parentJobIDs
    TimeUnit walltime
    Integer maxCpus
    Integer maxNodes
    Integer maxMemory //Total resident maxMemory usage of all processes in a job
    BufferUnit memoryBufferUnit
    String queue
    String otherSettings

    String numProcessors;
    String userTime; //user time used
    String systemTime; //system time used
    String runLimit;
    String pendReason;
    String execHome;
    String execUserName;
    String pidStr;
    String pgidStr;
    String nthreads; //Number of currently active threads of a job
    String swap; //Total virtual maxMemory (swap) usage of all processes in a job
    String exitStatus; // UNIX exit status of the job
    String jobGroup;
    String description;
    String execCwd; //Executed current working directory
    String askedHostsStr;
    String cwd; //Current working directory
    String projectName;

    String timeUserSuspState; //Suspended by its owner or the LSF administrator after being dispatched
    String timePendState; //Waiting in a queue for scheduling and dispatch
    String timePendSuspState; // Suspended by its owner or the LSF administrator while in PEND state
    String timeSystemSuspState; //Suspended by the LSF system after being dispatched
    String timeUnknownState;
    String timeOfCalculation;


    GenericJobInfo(String jobName, File tool, String id, Map<String, String> parameters, List<String> parentJobIDs) {
        this.jobName = jobName
        this.tool = tool
        this.id = id
        this.parameters = parameters
        this.parentJobIDs = parentJobIDs
    }

    @Override
    public String toString() {
        return "GenericJobInfo{" +
                "jobName='" + jobName + '\'' +
                ", tool='" + tool + '\'' +
                ", id='" + id + '\'' +
                ", parameters=" + parameters +
                ", parentJobIDs=" + parentJobIDs +
                ", maxMemory=" + maxMemory +
                ", maxCpus=" + maxCpus +
                ", maxNodes=" + maxNodes +
                ", queue=" + queue +
                ", otherSettings=" + otherSettings +
                ", user=" + user +
                ", subHost=" + subHost +
                ", exHosts=" + exHosts +
                ", runTime=" + runTime +
                ", subTime=" + subTime +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", numProcessors=" + numProcessors +
                ", cpuTime=" + cpuTime +
                ", userTime=" + userTime +
                ", systemTime=" + systemTime +
                ", runLimit=" + runLimit +
                ", pendReason=" + pendReason +
                ", priority=" + priority +
                ", userGroup=" + userGroup +
                ", resReq=" + resReq +
                ", execHome=" + execHome +
                ", execUserName=" + execUserName +
                ", pidStr=" + pidStr +
                ", pgidStr=" + pgidStr +
                ", nthreads=" + nthreads +
                ", swap=" + swap +
                ", exitStatus=" + exitStatus +
                ", jobGroup=" + jobGroup +
                ", description=" + description +
                ", execCwd=" + execCwd +
                ", askedHostsStr=" + askedHostsStr +
                ", cwd=" + cwd +
                ", projectName=" + projectName +
                ", outFile=" + outFile +
                ", inFile=" + inFile +
                ", timeUserSuspState=" + timeUserSuspState +
                ", timePendState=" + timePendState +
                ", timePendSuspState=" + timePendSuspState +
                ", timeSystemSuspState=" + timeSystemSuspState +
                ", timeUnknownState=" + timeUnknownState +
                ", timeOfCalculation=" + timeOfCalculation +
                '}';
    }
}
