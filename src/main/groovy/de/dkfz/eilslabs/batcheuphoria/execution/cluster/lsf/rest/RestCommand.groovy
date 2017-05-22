/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest

import de.dkfz.eilslabs.batcheuphoria.jobs.Command
import groovy.transform.CompileStatic
import org.apache.http.Header

/**
 * This class is used to create REST commands
 * Created by kaercher on 02.03.17.
 */
@CompileStatic
class RestCommand extends Command{

    String resource
    String requestBody
    List<Header> requestHeaders
    Enum<HttpMethod> httpMethod

    enum HttpMethod{
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
    RestCommand(String resource, String requestBody, List<Header> requestHeaders, Enum<HttpMethod> httpMethod){
        super(null,null,null,null,null)
        this.resource = resource
        this.requestBody = requestBody
        this.requestHeaders = requestHeaders
        this.httpMethod = httpMethod
    }
}
