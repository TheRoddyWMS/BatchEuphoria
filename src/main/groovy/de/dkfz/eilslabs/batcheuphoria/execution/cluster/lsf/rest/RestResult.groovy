package de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest

import de.dkfz.eilslabs.batcheuphoria.jobs.Job
import org.apache.http.Header

/**
 * Created by kaercher on 01.03.17.
 */
class RestResult {

    Header[] headers
    String body
    int statusCode
    Job job
}
