/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster

import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSCommand
import de.dkfz.roddy.tools.LoggerWrapper

/**
 * A class for processing backends running on a cluster.
 * This mainly defines variables and constants which can be set via the config.
 */
public abstract class ClusterJobManager<C extends Command> extends BatchEuphoriaJobManager<C> {
    private static final LoggerWrapper logger = LoggerWrapper.getLogger(BatchEuphoriaJobManager.class.getSimpleName());

    public static final String CVALUE_ENFORCE_SUBMISSION_TO_NODES="enforceSubmissionToNodes";

    ClusterJobManager(BEExecutionService executionService, JobManagerCreationParameters parms) {
        super(executionService, parms)
    }

    @Override
    int waitForJobsToFinish() {
        logger.info("The user requested to wait for all jobs submitted by this process to finish.")
        List<String> ids = new LinkedList<>()
//        List<ExecutionContext> listOfContexts = new LinkedList<>();
        synchronized (listOfCreatedCommands) {
            for (Object _command : listOfCreatedCommands) {
                PBSCommand command = (PBSCommand) _command
                if (command.getJob() instanceof FakeBEJob)
                    continue
                ids.add(command.getExecutionID().getShortID())
//                ExecutionContext context = command.getExecutionContext();
//                if (!listOfContexts.contains(context)) {
//                    listOfContexts.add(context);
//                }
            }
        }

        boolean isRunning = true
        while (isRunning) {

            isRunning = false
            Map<String, JobState> stringJobStateMap = queryJobStatus(ids, true)
            if (logger.isVerbosityHigh()) {
                for (String s : stringJobStateMap.keySet()) {
                    if (stringJobStateMap.get(s) != null)
                        System.out.println(s + " = " + stringJobStateMap.get(s))
                }
            }
            for (JobState js : stringJobStateMap.values()) {
                if (js == null) //Only one job needs to be active.
                    continue

                if (js.isPlannedOrRunning()) {
                    isRunning = true
                    break
                }
            }
            if (isRunning) {
                try {
                    logger.info("Waiting for jobs to finish.")
                    Thread.sleep(5000) //Sleep one minute until the next query.
                } catch (InterruptedException e) {
                    e.printStackTrace()
                }
            } else {
                logger.info("Finished waiting")
            }
        }
        int errnousJobs = 0
//        for (ExecutionContext context : listOfContexts) {
//            for (BEJob job : context.getExecutedJobs())
//                if (job.getJobID() != null) errnousJobs++; //Skip null jobs.
//
//            Map<String, JobState> statesMap = context.getRuntimeService().readInJobStateLogFile(context);
//            statesMap.each {
//                String s, JobState integer ->
//                    if (integer == 0)
//                        errnousJobs--;
//                    else
//                        logger.info("BEJob " + s + " exited with an error.");
//            }
//            int unknown = context.getExecutedJobs().size() - statesMap.size();
//            if (unknown > 0) {
//                logger.info("There were " + unknown + " jobs with an unknown jobState.");
////                for (String s : statesMap.keySet()) {
////                    logger.info("\t" + s + " => " + statesMap.get(s));
////                }
//            }
//        }

        return errnousJobs
    }

}
