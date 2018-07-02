package de.dkfz.roddy.execution.jobs.cluster

import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.ProcessingParameters
import de.dkfz.roddy.execution.jobs.SubmissionCommand
import groovy.transform.CompileStatic


@CompileStatic
abstract class GridEngineBasedCommand extends SubmissionCommand {

    public static final String NONE = "none"

    /**
     * A command to be executed on the cluster head node, in particular qsub, bsub, qstat, etc.
     *
     * @param parentJobManager
     * @param job
     * @param jobName
     * @param processingParameter s@param environmentVariables *@param dependencyIDs @param command
     */
    GridEngineBasedCommand(BatchEuphoriaJobManager parentJobManager, BEJob job, String jobName, List<ProcessingParameters> processingParameters, Map<String, String> environmentVariables, List<String> dependencyIDs, String command) {
        super(parentJobManager, job, jobName, processingParameters, environmentVariables, dependencyIDs, command)
    }


    @Override
    protected String assembleDependencyString(List<BEJobID> jobIds) {
        StringBuilder qsubCall = new StringBuilder("")
        LinkedList<String> tempDependencies =
                jobIds.findAll {
                    it.getId() != "" && it.getId() != NONE && it.getId() != "-1"
                }.collect {
                    it.getId().split("\\.")[0] // Keep the command line short. GE accepts the job number for dependencies.
                } as LinkedList<String>
        if (tempDependencies.size() > 0) {
            qsubCall <<
                    getDependsSuperParameter() <<
                    getDependencyParameterName() <<
                    getDependencyOptionSeparator() <<
                    tempDependencies.join(getDependencyIDSeparator())
        }

        return qsubCall
    }

    protected abstract String getDependsSuperParameter()

    protected abstract String getDependencyParameterName()

    protected abstract String getDependencyOptionSeparator()

    protected abstract String getDependencyIDSeparator()
}
