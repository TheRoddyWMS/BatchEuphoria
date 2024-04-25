/*
 * Copyright (c) 2021 .
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf.rest


import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.tools.AnyEscapableString
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.SubmissionCommand
import groovy.transform.CompileStatic
import org.apache.http.Header
import sun.reflect.generics.reflectiveObjects.NotImplementedException

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
        super(null, null, null, null, null)
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
    protected AnyEscapableString assembleVariableExportParameters() {
        throw new NotImplementedException()
    }

    @Override
    protected AnyEscapableString getAdditionalCommandParameters() {
        throw new NotImplementedException()
    }

    @Override
    protected AnyEscapableString getEnvironmentString() {
        return LSFRestJobManager.environmentString
    }

    @Override
    protected AnyEscapableString getWorkingDirectoryParameter() {
        throw new NotImplementedException()
    }

    @Override
    protected AnyEscapableString assembleDependencyParameter(List<BEJobID> jobIds) {
        throw new NotImplementedException()
    }

    @Override
    protected AnyEscapableString getUmaskString(AnyEscapableString umask) {
        throw new NotImplementedException()
    }

    @Override
    protected AnyEscapableString getJobNameParameter() {
        throw new NotImplementedException()
    }

    @Override
    protected AnyEscapableString getGroupListParameter(AnyEscapableString groupList) {
        throw new NotImplementedException()
    }

    @Override
    protected AnyEscapableString getHoldParameter() {
        throw new NotImplementedException()
    }

    @Override
    protected AnyEscapableString getEmailParameter(AnyEscapableString address) {
        throw new NotImplementedException()
    }

    @Override
    protected AnyEscapableString getLoggingParameter(JobLog jobLog) {
        throw new NotImplementedException()
    }

    @Override
    protected Boolean getQuoteCommand() {
        false
    }


    @Override
    protected String composeCommandString(List<AnyEscapableString> parameters) {
        throw new NotImplementedException()
    }

}
