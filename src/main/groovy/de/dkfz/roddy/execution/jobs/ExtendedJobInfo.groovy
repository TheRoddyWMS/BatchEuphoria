/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.config.ResourceSet
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

import java.time.Duration
import java.time.ZonedDateTime

/**
 * Stores extended information like e.g. statistical data about a job
 *
 * Note, that the amount of gathered information depends on the target system and can vary with different job states.
 * E.g. the fields
 *
 * This class is a value class. Normally, we'd use final fields for all values or some kind of
 * builder interface to create instances of it. But due to the sheer amount of its fields,
 * we will refrain from both. Please make sure, that you do net mess things up.
 */
@CompileStatic
@ToString(includeNames = true)
@EqualsAndHashCode(callSuper = true)
class ExtendedJobInfo extends JobInfo {

    // Common information

    String jobName

    String user

    String userGroup

    /**
     * Umask with which files and directories are created
     */
    String umask

    String execUserName

    String description

    String projectName

    String jobGroup

    /**
     * The executed command
     */
    String command

    /**
     * Job parameters during submission
     */
    Map<String, String> parameters

    /**
     * IDs of the jobs parent jobs
     */
    List<String> parentJobIDs

    String priority

    /**
     * List of process ids within the job
     */
    List<String> processesInJob

    /**
     * Currently active process group ID in a job.
     */
    String processGroupID

    String submissionHost

    String account

    /**
     * Submission server
     */
    String server

    // Resources

    String rawHostQueryString

    List<String> executionHosts

    /**
     * Requested resources like e.g. walltime, max memory
     */
    ResourceSet requestedResources

    /**
     * Used resources like e.g. walltime, max memory
     */
    ResourceSet usedResources

    /**
     * resource requirements
     */
    String rawResourceRequest

    /**
     * Time in seconds that the job has been in the run state
     */
    Duration runTime

    /**
     * Cumulative total CPU time in seconds of all processes in a job
     */
    Duration cpuTime

    // Status

    /**
     * (UNIX) exit status of the job
     */
    Integer exitCode

    /**
     * Reason, why a job is on hold or suspended
     */
    String pendReason

    /**
     * How often was the job started
     */
    Integer startCount

    // Directories

    /**
     * Current working directory
     */
    String cwd

    /**
     * Executed current working directory
     */
    String execCwd

    String execHome

    File logFile

    File errorLogFile

    File inputFile

    // Timestamps and timing info

    /**
     * user time used
     */
    String userTime

    /**
     * system time used
     */
    String systemTime

    /**
     * The date-time the job entered the queue.
     */
    ZonedDateTime submitTime

    /**
     * The date-time the job became eligible to run when all conditions like job dependencies are met, i.e. in a queued state while residing in an execution queue.
     */
    ZonedDateTime eligibleTime

    /**
     * The date-time the job was started.
     */
    ZonedDateTime startTime

    /**
     * Suspended by its owner or the LSF administrator after being dispatched
     */
    Duration timeUserSuspState

    /**
     * Waiting in a queue for scheduling and dispatch
     */
    Duration timePendState

    /**
     * Suspended by its owner or the LSF administrator while in PEND state
     */
    Duration timePendSuspState

    /**
     * Suspended by the job system after being dispatched
     */
    Duration timeSystemSuspState

    Duration timeUnknownState

    ZonedDateTime timeOfCalculation

    // Whatever...

    /**
     * Guess what... don't know
     */
    String otherSettings


    ExtendedJobInfo(BEJobID jobID) {
        super(jobID)
    }

    ExtendedJobInfo(BEJobID jobID, JobState jobState) {
        super(jobID)
        this.jobState = jobState
    }

    @Deprecated
    ExtendedJobInfo(String jobName, String command, BEJobID jobID, Map<String, String> parameters, List<String> parentJobIDs) {
        super(jobID)
        this.jobName = jobName
        this.command = command
        this.jobID = jobID
        this.parameters = parameters
        this.parentJobIDs = parentJobIDs
    }

}
