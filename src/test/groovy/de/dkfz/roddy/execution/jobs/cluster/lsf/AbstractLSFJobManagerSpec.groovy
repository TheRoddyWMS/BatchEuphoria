package de.dkfz.roddy.execution.jobs.cluster.lsf

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.JobManagerOptionsBuilder
import spock.lang.Shared
import spock.lang.Specification

import java.time.Duration

class AbstractLSFJobManagerSpec extends Specification {

    @Shared
    AbstractLSFJobManager jobManager

    def setupSpec() {
        jobManager = new LSFJobManager(new BEExecutionService() {
            @Override
            ExecutionResult execute(Command command) {
                return null
            }

            @Override
            ExecutionResult execute(Command command, boolean waitFor) {
                return null
            }

            @Override
            ExecutionResult execute(Command command, Duration timeout) {
                return null
            }

            @Override
            ExecutionResult execute(Command command, boolean waitFor, Duration timeout) {
                return null
            }

            @Override
            ExecutionResult execute(String command) {
                return null
            }

            @Override
            ExecutionResult execute(String command, boolean waitFor) {
                return null
            }

            @Override
            ExecutionResult execute(String command, Duration timeout) {
                return null
            }

            @Override
            ExecutionResult execute(String command, boolean waitFor, Duration timeout) {
                return null
            }

            @Override
            boolean isAvailable() {
                return false
            }

            @Override
            File queryWorkingDirectory() {
                return null
            }
        }, new JobManagerOptionsBuilder().build())
    }

    def "test conversion of core and node resources with createComputeParameter"(ResourceSet input, LinkedHashMap results) {
        when:
        LinkedHashMultimap<String, String> parameters = new LinkedHashMultimap<>(1, 1)
        jobManager.createComputeParameter(input, parameters)

        then:
        // You cannot simply compare a LinkedHashMultimap with == so we compare element by element
        // You also cannot use Groovy methods on the class
        parameters.size() == results.size()
        // It is also incredibly hard to compare the LinkedHashMultimap with "normal" maps.
        results["-n"].toString() == parameters.get("-n").toString()

        where:
        input                                                  | results
        new ResourceSet(null, 4, -2, null, null, null, null)   | [("-n"): ['4 -R "span[hosts=1]"']]
        new ResourceSet(null, 4, null, null, null, null, null) | [("-n"): ['4 -R "span[hosts=1]"']]
        new ResourceSet(null, 4, 0, null, null, null, null)    | [("-n"): ['4 -R "span[hosts=1]"']]
        new ResourceSet(null, 4, 1, null, null, null, null)    | [("-n"): ['4 -R "span[hosts=1]"']]
        new ResourceSet(null, 4, 2, null, null, null, null)    | [("-n"): ['8 -R "span[ptile=4]"']]
    }
}
