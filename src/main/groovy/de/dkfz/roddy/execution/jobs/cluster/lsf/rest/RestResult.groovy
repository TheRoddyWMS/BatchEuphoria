/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf.rest

import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.io.ExecutionResult
import groovy.transform.CompileStatic
import org.apache.http.Header

/**
 * Represents the respond of a REST service request
 * Created by kaercher on 01.03.17.
 */
@CompileStatic
class RestResult extends ExecutionResult {

    Header[] headers
    BEJob job

    /**
     * Represents the respond of a REST service request
     * @param headers - headers of the respond
     * @param body - body of the respond
     * @param statusCode - http status code
     */
    RestResult(RestSubmissionCommand command, Header[] headers, String body, int statusCode) {
        super(commandSummaryList(command), statusCode == 200, statusCode, [body], [], null)
        this.headers = headers
    }

    @Override
    boolean isSuccessful() {
        return statusCode == 200
    }

    int getStatusCode() {
        exitCode
    }

    String getBody() {
        stdout.join("\n")
    }

    private static List<String> commandSummaryList(RestSubmissionCommand command) {
        List<String> summary =
                ["resource=$command.resource",
                 "method=$command.httpMethod",
                 "headers=[" +
                         command.requestHeaders.collect {
                             "${it.name}=${it.value}"
                         }.join(", ") + "]",
                 "body=$command.requestBody"] as List<String>
        return summary
    }


}
