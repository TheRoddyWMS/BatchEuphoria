/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.slurm


import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.ComplexLine
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic

import static de.dkfz.roddy.StringConstants.SPLIT_COMMA
import static de.dkfz.roddy.StringConstants.SPLIT_EQUALS

/**
 * Used to convert commands from cli to e.g. GenericJobInfo
 *
 */
@CompileStatic
class SlurmCommandParser {

    String[] options
    String jobName
    String walltime
    String memory
    BufferUnit bufferUnit = BufferUnit.G
    String cores
    String nodes
    String queue
    String otherSettings
    String script
    BEJobID jobID
    Map<String, String> parameters = new LinkedHashMap<>()
    List<String> dependencies = new LinkedList<>()

    String commandString

    SlurmCommandParser(String commandString) {
        this.commandString = commandString
        parse()
    }

    void parse() {
        commandString = commandString.trim()
        // Get the job id
        jobID = new BEJobID(commandString.substring(0, commandString.indexOf(",")))
        // Get rid of the job id
        commandString = commandString.substring(commandString.indexOf(",") + 1).trim()

        // Create a complex line object which will be used for further parsing.
        ComplexLine line = new ComplexLine(commandString)

        if (!commandString.startsWith("qsub")) return  // It is obviously not a PBS call

        Collection<String> splitted = line.splitBy(" ").findAll { it }
        script = splitted[-1]
        jobName = "not readable"

        for (int i = 0; i < splitted.size(); i++) {
            String option = splitted[i]
            if (!option.startsWith("--")) continue // It is not an option but a parameter or a text (e.g. qsub, script)

            if (option.startsWith("--job-name")) {
                jobName = option.split("=")[1]
            } else if (option.startsWith("--export")) {
                parseVariables(option.split("=")[1])
            }
        }
    }

    private void parseVariables(String parameters) {
        String[] variables = parameters.split(SPLIT_COMMA)
        for (String variable : variables) {
            String[] varSplit = variable.split(SPLIT_EQUALS)
            String header = varSplit[0]
            String value = "UNKNOWN"
            if (varSplit.length > 1)
                value = varSplit[1]
            this.parameters.put(header, value)
        }
    }

    private void parseResources(String parameters) {
        def splitParms = parameters.split("[,]")
    }

    private void parseDependencies(String parameters) {

    }

    GenericJobInfo toGenericJobInfo() {
        GenericJobInfo jobInfo = new GenericJobInfo(jobName, new File(script), jobID, parameters, dependencies)
        ResourceSet askedResources = new ResourceSet(null, memory ? new BufferValue(memory as Integer, bufferUnit) : null,
                cores ? cores as Integer : null, nodes ? nodes as Integer : null, walltime ? new TimeUnit(walltime) : null,
                null, null, null)
        jobInfo.setAskedResources(askedResources)
        return jobInfo
    }
}
