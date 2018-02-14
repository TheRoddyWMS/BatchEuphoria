/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */
package de.dkfz.roddy.config

import groovy.transform.CompileStatic


/**
 * This class is used to pass how to write the log output to the BEJob constructor.
 * You can either have it not write a log file, a log file with combined stdout and stderr or two log files with separate stdout and stderr.
 *
 */
@CompileStatic
class JobLog {
    public static final String JOB_ID = '{JOB_ID}'

    /**
     * Don't write any log files
     */
    static JobLog none() {
        new JobLog(null, null)
    }

    /**
     * Write standard out and error to the log file given as @param log.
     * If log is a relative path, it is relative to the working directory.
     * If log is a directory, the log file is written in this directory with a cluster scheduler specific file name.
     * If log is a file, the log file is written in this file; the parent directory must exist, otherwise nothing is written.
     * {@link #JOB_ID} can be used as a placeholder which will be replaced by the job ID.
     *
     * NB. On different systems may be written on the execution host or on the submission host.
     * (On LSF files are written on the execution host, on PBS on the submission host)
     * If you pass a directory on LSF, it should be mounted on both the submission and execution host, otherwise BE won't be able to return the correct path
     * @param log
     * @return
     */
    static JobLog toOneFile(File log) {
        new JobLog(log, log)
    }

    /**
     * Write standard out and error to the two given files.
     * See {@link #toOneFile} how file names are used.
     * @param out
     * @param error
     * @return
     */
    static JobLog toSeparateStdoutAndError(File out, File error) {
        new JobLog(out, error)
    }


    private File out
    private File error
    private JobLog(File out, File err) {
        this.out = out
        this.error = err
    }
    String getOut() {
        out?.path
    }

    String getOut(String jobID) {
        out?.path.toString().replace(JOB_ID, jobID)
    }

    String getError() {
        error?.path
    }

    String getError(String jobID) {
        error?.path.toString().replace(JOB_ID, jobID)
    }

}
