package de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest

import de.dkfz.eilslabs.batcheuphoria.jobs.Command
import org.apache.http.Header

/**
 * This class is used to create REST commands
 * Created by kaercher on 02.03.17.
 */
class RestCommand extends Command{

    String resource
    String requestBody
    List<Header> requestHeaders
    Enum<HttpMethod> httpMethod

    enum HttpMethod{
        HTTPPOST,
        HTTPGET
    }

    RestCommand(String resource, String requestBody, List<Header> requestHeaders, Enum<HttpMethod> httpMethod){
        super(null,null,null,null,null)
        this.resource = resource
        this.requestBody = requestBody
        this.requestHeaders = requestHeaders
        this.httpMethod = httpMethod
    }
}
