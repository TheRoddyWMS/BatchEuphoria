/*
 * Copyright (c) 2021 .
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf.rest


import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.tools.EscapableString
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
    protected EscapableString assembleVariableExportParameters() {
        throw new NotImplementedException()
    }

    @Override
    protected EscapableString getAdditionalCommandParameters() {
        throw new NotImplementedException()
    }

    @Override
    protected EscapableString getEnvironmentString() {
        return LSFRestJobManager.environmentString
    }

    @Override
    protected EscapableString getWorkingDirectoryParameter() {
        throw new NotImplementedException()
    }

    @Override
    protected EscapableString assembleDependencyParameter(List<BEJobID> jobIds) {
        throw new NotImplementedException()
    }

    @Override
    protected EscapableString getUmaskString(EscapableString umask) {
        throw new NotImplementedException()
    }

    @Override
    protected EscapableString getJobNameParameter() {
        throw new NotImplementedException()
    }

    @Override
    protected EscapableString getGroupListParameter(EscapableString groupList) {
        throw new NotImplementedException()
    }

    @Override
    protected EscapableString getHoldParameter() {
        throw new NotImplementedException()
    }

    @Override
    protected EscapableString getEmailParameter(EscapableString address) {
        throw new NotImplementedException()
    }

    @Override
    protected EscapableString getLoggingParameter(JobLog jobLog) {
        throw new NotImplementedException()
    }

    @Override
    protected Boolean getQuoteCommand() {
        false
    }


    @Override
    protected String composeCommandString(List<EscapableString> parameters) {
        throw new NotImplementedException()
    }

}
