/*
 * Copyright (c) 2016 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.jobs;


import de.dkfz.roddy.tools.BufferUnit;

import java.util.List;
import java.util.Map;

/**
 * Created by michael on 06.02.15.
 */
public class GenericJobInfo {

    private final String jobName;
    private final String toolID;
    private final String id;
    private final Map<String, String> parameters;
    private List<String> parentJobIDs;
    private String walltime;
    private String memory;
    private BufferUnit memoryBufferUnit;
    private String cpus;
    private String nodes;
    private String queue;
    private String otherSettings;
    private String user;
    private String subHost; //submission host
    private String exHosts; // execution hosts
    private String runTime; //Time in seconds that the job has been in the run state
    private String subTimeGMT;
    private String startTimeGMT;
    private String endTimeGMT;
    private String numProcessors;
    private String cpuTime;
    private String userTime; //user time used
    private String systemTime; //system time used
    private String runLimit;
    private String pendReason;
    private String priority;
    private String userGroup;
    private String resReq; // resource requirements
    private String execHome;
    private String execUserName;
    private String pidStr;
    private String pgidStr;
    private String nthreads; //The number of threads per core configured on a host
    private String swap; //vmem
    private String exitStatus; // UNIX exit status of the job
    private String jobGroup;
    private String description;
    private String execCwd; //Executed current working directory
    private String askedHostsStr;
    private String cwd; //Current working directory
    private String projectName;
    private String outfile;
    private String infile;
    private String timeUserSuspState; //Suspended by its owner or the LSF administrator after being dispatched
    private String timePendState; //Waiting in a queue for scheduling and dispatch
    private String timePendSuspState; // Suspended by its owner or the LSF administrator while in PEND state
    private String timeSystemSuspState; //Suspended by the LSF system after being dispatched
    private String timeUnknownState;
    private String timeOfCalculation;

    public GenericJobInfo(String jobName, String toolID, String id, Map<String, String> parameters, List<String> parentJobIDs) {

        this.jobName = jobName;
        this.toolID = toolID;
        this.id = id;
        this.parameters = parameters;
        this.parentJobIDs = parentJobIDs;
    }

    public String getJobName() {
        return jobName;
    }

    public String getToolID() {
        return toolID;
    }

    public String getID() {
        return id;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public List<String> getParentJobIDs() {
        return parentJobIDs;
    }

    public void setWalltime(String walltime) {
        this.walltime = walltime;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public void setMemoryBufferUnit(BufferUnit memoryBufferUnit) {
        this.memoryBufferUnit = memoryBufferUnit;
    }

    public void setCpus(String cpus) {
        this.cpus = cpus;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public void setOtherSettings(String otherSettings) {
        this.otherSettings = otherSettings;
    }

    public void setParentJobIDs(List<String> parentJobIDs) {
        this.parentJobIDs = parentJobIDs;
    }

    public String getId() {
        return id;
    }

    public String getWalltime() {
        return walltime;
    }

    public String getMemory() {
        return memory;
    }

    public BufferUnit getMemoryBufferUnit() {
        return memoryBufferUnit;
    }

    public String getCpus() {
        return cpus;
    }

    public String getNodes() {
        return nodes;
    }

    public String getQueue() {
        return queue;
    }

    public String getOtherSettings() {
        return otherSettings;
    }



    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getSubHost() {
        return subHost;
    }

    public void setSubHost(String subHost) {
        this.subHost = subHost;
    }

    public String getExHosts() {
        return exHosts;
    }

    public void setExHosts(String exHosts) {
        this.exHosts = exHosts;
    }

    public String getRunTime() {
        return runTime;
    }

    public void setRunTime(String runTime) {
        this.runTime = runTime;
    }

    public String getSubTimeGMT() {
        return subTimeGMT;
    }

    public void setSubTimeGMT(String subTimeGMT) {
        this.subTimeGMT = subTimeGMT;
    }

    public String getStartTimeGMT() {
        return startTimeGMT;
    }

    public void setStartTimeGMT(String startTimeGMT) {
        this.startTimeGMT = startTimeGMT;
    }

    public String getEndTimeGMT() {
        return endTimeGMT;
    }

    public void setEndTimeGMT(String endTimeGMT) {
        this.endTimeGMT = endTimeGMT;
    }

    public String getNumProcessors() {
        return numProcessors;
    }

    public void setNumProcessors(String numProcessors) {
        this.numProcessors = numProcessors;
    }

    public String getCpuTime() {
        return cpuTime;
    }

    public void setCpuTime(String cpuTime) {
        this.cpuTime = cpuTime;
    }

    public String getUserTime() {
        return userTime;
    }

    public void setUserTime(String userTime) {
        this.userTime = userTime;
    }

    public String getSystemTime() {
        return systemTime;
    }

    public void setSystemTime(String systemTime) {
        this.systemTime = systemTime;
    }

    public String getRunLimit() {
        return runLimit;
    }

    public void setRunLimit(String runLimit) {
        this.runLimit = runLimit;
    }

    public String getPendReason() {
        return pendReason;
    }

    public void setPendReason(String pendReason) {
        this.pendReason = pendReason;
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    public String getUserGroup() {
        return userGroup;
    }

    public void setUserGroup(String userGroup) {
        this.userGroup = userGroup;
    }

    public String getResReq() {
        return resReq;
    }

    public void setResReq(String resReq) {
        this.resReq = resReq;
    }

    public String getExecHome() {
        return execHome;
    }

    public void setExecHome(String execHome) {
        this.execHome = execHome;
    }

    public String getExecUserName() {
        return execUserName;
    }

    public void setExecUserName(String execUserName) {
        this.execUserName = execUserName;
    }

    public String getPidStr() {
        return pidStr;
    }

    public void setPidStr(String pidStr) {
        this.pidStr = pidStr;
    }

    public String getPgidStr() {
        return pgidStr;
    }

    public void setPgidStr(String pgidStr) {
        this.pgidStr = pgidStr;
    }

    public String getNthreads() {
        return nthreads;
    }

    public void setNthreads(String nthreads) {
        this.nthreads = nthreads;
    }

    public String getSwap() {
        return swap;
    }

    public void setSwap(String swap) {
        this.swap = swap;
    }

    public String getExitStatus() {
        return exitStatus;
    }

    public void setExitStatus(String exitStatus) {
        this.exitStatus = exitStatus;
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public void setJobGroup(String jobGroup) {
        this.jobGroup = jobGroup;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getExecCwd() {
        return execCwd;
    }

    public void setExecCwd(String execCwd) {
        this.execCwd = execCwd;
    }

    public String getAskedHostsStr() {
        return askedHostsStr;
    }

    public void setAskedHostsStr(String askedHostsStr) {
        this.askedHostsStr = askedHostsStr;
    }

    public String getCwd() {
        return cwd;
    }

    public void setCwd(String cwd) {
        this.cwd = cwd;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getOutfile() {
        return outfile;
    }

    public void setOutfile(String outfile) {
        this.outfile = outfile;
    }

    public String getInfile() {
        return infile;
    }

    public void setInfile(String infile) {
        this.infile = infile;
    }

    public String getTimeUserSuspState() {
        return timeUserSuspState;
    }

    public void setTimeUserSuspState(String timeUserSuspState) {
        this.timeUserSuspState = timeUserSuspState;
    }

    public String getTimePendState() {
        return timePendState;
    }

    public void setTimePendState(String timePendState) {
        this.timePendState = timePendState;
    }

    public String getTimePendSuspState() {
        return timePendSuspState;
    }

    public void setTimePendSuspState(String timePendSuspState) {
        this.timePendSuspState = timePendSuspState;
    }

    public String getTimeSystemSuspState() {
        return timeSystemSuspState;
    }

    public void setTimeSystemSuspState(String timeSystemSuspState) {
        this.timeSystemSuspState = timeSystemSuspState;
    }

    public String getTimeUnknownState() {
        return timeUnknownState;
    }

    public void setTimeUnknownState(String timeUnknownState) {
        this.timeUnknownState = timeUnknownState;
    }

    public String getTimeOfCalculation() {
        return timeOfCalculation;
    }

    public void setTimeOfCalculation(String timeOfCalculation) {
        this.timeOfCalculation = timeOfCalculation;
    }

    @Override
    public String toString() {
        return "GenericJobInfo{" +
                "jobName='" + jobName + '\'' +
                ", toolID='" + toolID + '\'' +
                ", id='" + id + '\'' +
                ", parameters=" + parameters +
                ", parentJobIDs=" + parentJobIDs +
                ", memory=" + memory  +
                ", cpus=" + cpus +
                ", nodes=" + nodes  +
                ", queue=" + queue +
                ", otherSettings=" + otherSettings +
                ", user=" + user  +
                ", subHost=" + subHost +
                ", exHosts=" + exHosts +
                ", runTime=" + runTime +
                ", subTimeGMT=" + subTimeGMT +
                ", startTimeGMT=" + startTimeGMT +
                ", endTimeGMT=" + endTimeGMT  +
                ", numProcessors=" + numProcessors   +
                ", cpuTime=" + cpuTime  +
                ", userTime=" + userTime +
                ", systemTime=" + systemTime  +
                ", runLimit=" + runLimit +
                ", pendReason=" + pendReason +
                ", priority=" + priority  +
                ", userGroup=" + userGroup +
                ", resReq=" + resReq +
                ", execHome=" + execHome +
                ", execUserName=" + execUserName +
                ", pidStr=" + pidStr +
                ", pgidStr=" + pgidStr +
                ", nthreads=" + nthreads +
                ", swap=" + swap +
                ", exitStatus=" + exitStatus  +
                ", jobGroup=" + jobGroup  +
                ", description=" + description +
                ", execCwd=" + execCwd  +
                ", askedHostsStr=" + askedHostsStr +
                ", cwd=" + cwd +
                ", projectName=" + projectName  +
                ", outfile=" + outfile +
                ", infile=" + infile  +
                ", timeUserSuspState=" + timeUserSuspState  +
                ", timePendState=" + timePendState +
                ", timePendSuspState=" + timePendSuspState +
                ", timeSystemSuspState=" + timeSystemSuspState  +
                ", timeUnknownState=" + timeUnknownState  +
                ", timeOfCalculation=" + timeOfCalculation  +
                '}';
    }
}
