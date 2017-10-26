package de.dkfz.roddy.execution.jobs.cluster.lsf

import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.ComplexLine
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic
import static de.dkfz.roddy.StringConstants.SPLIT_COLON
import static de.dkfz.roddy.StringConstants.SPLIT_COMMA
import static de.dkfz.roddy.StringConstants.SPLIT_EQUALS

/**
 * Used to convert commands from cli to e.g. GenericJobInfo
 * Created by kaercher on 15.05.17.
 */
@CompileStatic
class LSFCommandParser {

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
    String id
    Map<String, String> parameters = new LinkedHashMap<>()
    List<String> dependencies = new LinkedList<>()

    String commandString

    LSFCommandParser(String commandString) {
        this.commandString = commandString
        parse()
    }

    void parse() {
        commandString = commandString.trim()
        // Get the job id
        id = commandString.substring(0, commandString.indexOf(","))
        // Get rid of the job id
        commandString = commandString.substring(commandString.indexOf(",") + 1).trim()

        // Create a complex line object which will be used for further parsing.
        ComplexLine line = new ComplexLine(commandString)

        if (!commandString.startsWith("bsub")) return  // It is obviously not a PBS call

        String[] splitted = line.splitBy(" ").findAll { it }
        script = splitted[-1]
        jobName = "not readable"

        for (int i = 0; i < splitted.length - 1; i++) {
            String option = splitted[i]
            if (!option.startsWith("-")) continue // It is not an option but a parameter or a text (e.g. bsub, script)

            String parms = splitted[i + 1]
            if (option == "-N") {
                jobName = parms
            } else if (option == "-env") {
                parseVariables(parms)
            } else if (option == "-M" && option == "-W" && option == "-n") { //others
                parseResources(parms)
            } else if (option == "-W depend=") {
                parseDependencies(parms)
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
        splitParms.each {
            String parm ->
                String parmID = parm.split(SPLIT_EQUALS)[0]
                String parmVal = parm.split(SPLIT_EQUALS)[1]
                if (parmID == "mem") {
                    bufferUnit = BufferUnit.valueOf(parmVal[-1])
                    memory = parmVal[0..-2]
                } else if (parmID == "walltime") {
                    walltime = parameters.split("[=]")[1]
                } else if (parmID == "maxNodes") {
                    String[] splitParm = parm.split(SPLIT_COLON)
                    for (String resource : splitParm) {
                        String[] splitResource = resource.split(SPLIT_EQUALS)
                        if (splitResource[0] == "maxNodes") {
                            nodes = splitResource[1]
                        } else if (splitResource[0] == "ppn") {
                            cores = splitResource[1]
                        } else {
                            // A node might be fixed. How to deal with it?
//                            fixedNode =
                        }
                    }
                }
        }
    }

    private void parseDependencies(String parameters) {
        if (parameters.startsWith("-W depend=")) {
            def deps = parameters[7..-1].split("[:]")
            if (deps[0] != "afterok")
                throw new BEException("Not supported: " + deps[0])
            dependencies.addAll(deps[1..-1])
        }
    }

    GenericJobInfo toGenericJobInfo() {
        GenericJobInfo jInfo = new GenericJobInfo(jobName, new File(script), id, parameters, dependencies)
        ResourceSet askedResources = new ResourceSet(null, memory ? new BufferValue(memory as Integer, bufferUnit) : null,
                cores ? cores as Integer : null, nodes ? nodes as Integer : null, walltime ? new TimeUnit(walltime) : null,
                null, null, null)
        jInfo.setAskedResources(askedResources)
        return jInfo
    }
}

