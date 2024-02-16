/*
 * Copyright (c) 2021 .
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf.rest

import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.SubmissionCommand
import de.dkfz.roddy.tools.BashUtils
import de.dkfz.roddy.tools.shell.bash.Service
import groovy.transform.CompileStatic
import org.apache.http.Header
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import static de.dkfz.roddy.StringConstants.EMPTY

/**
 * This class is used to create REST commands
 */
@CompileStatic
class RestSubmissionCommand extends SubmissionCommand {

    String resource
    String requestBody
    List<Header> requestHeaders
    Enum<HttpMethod> httpMethod

    enum HttpMethod {
        HTTPPOST,
        HTTPGET
    }
    /**
     * Used to send a web service request
     * @param resource - URI resource
     * @param requestBody - request body
     * @param requestHeaders - request headers
     * @param httpMethod - http Method POST or GET
     */
    RestSubmissionCommand(String resource, String requestBody, List<Header> requestHeaders, Enum<HttpMethod> httpMethod) {
        super(null, null, null, null, null,null)
        this.resource = resource
        this.requestBody = requestBody
        this.requestHeaders = requestHeaders
        this.httpMethod = httpMethod
    }

    @Override
    String getSubmissionExecutableName() {
        return null
    }

    @Override
    String toBashCommandString() {
        throw new RuntimeException("This method should not be used!")
    }

    @Override
    protected String assembleVariableExportParameters() {
        throw new NotImplementedException()
    }

    @Override
    protected String getAdditionalCommandParameters() {
        throw new NotImplementedException()
    }

    @Override
    protected String getEnvironmentString() {
        return LSFRestJobManager.environmentString
    }

    @Override
    protected String getWorkingDirectoryParameter() {
        throw new NotImplementedException()
    }

    @Override
    protected String assembleDependencyParameter(List<BEJobID> jobIds) {
        throw new NotImplementedException()
    }

    @Override
    protected String getUmaskString(String umask) {
        throw new NotImplementedException()
    }

    @Override
    protected String getJobNameParameter() {
        throw new NotImplementedException()
    }

    @Override
    protected String getGroupListParameter(String groupList) {
        throw new NotImplementedException()
    }

    @Override
    protected String getHoldParameter() {
        throw new NotImplementedException()
    }

    @Override
    protected String getEmailParameter(String address) {
        throw new NotImplementedException()
    }

    @Override
    protected String getLoggingParameter(JobLog jobLog) {
        throw new NotImplementedException()
    }

    @Override
    protected Boolean getQuoteCommand() {
        false
    }


    @Override
    protected String composeCommandString(List<String> parameters) {
        throw new NotImplementedException()
    }

}
