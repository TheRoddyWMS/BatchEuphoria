package de.dkfz.config

import de.dkfz.execution.jobs.Command

/**
 * Created by kaercher on 12.01.17.
 */
public interface ExecutionService {

    public void execute(Command command)

    public void execute(String command, boolean waitFor)


}