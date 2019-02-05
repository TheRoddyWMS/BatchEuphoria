package de.dkfz.roddy.execution.jobs.cluster

import spock.lang.Specification

class GridEngineBasedJobManagerSpec extends Specification {

    void "test getExecutionHosts"() {
        expect:
        output == GridEngineBasedJobManager.getExecutionHosts(input)

        where:
        input                                 || output
        null                                  || []
        ""                                    || []
        "asdf"                                || ["asdf"]
        "asdf+asdf"                           || ["asdf"]
        "asdf+qwertz"                         || ["asdf", "qwertz"]
        "asdf/5+asdf"                         || ["asdf"]
        "asdf/10+asdf/1"                      || ["asdf"]
        "asdf/3+qwertz/7"                     || ["asdf", "qwertz"]
        "asdf+qwertz/2"                       || ["asdf", "qwertz"]
        "asdf+qwertz/2+yxcv/6"                || ["asdf", "qwertz", "yxcv"]
        "exec-host/0+exec-host/1+exec-host/2" || ["exec-host"]

    }

    void "test getJobDependencies"() {
        expect:
        output == GridEngineBasedJobManager.getJobDependencies(input)

        where:
        input                                                                        || output
        null                                                                         || []
        ""                                                                           || []
        "asdf"                                                                       || []
        "beforeok:4356.server"                                                       || []
        "afterok:12341"                                                              || ["12341"]
        "afterok:12341.server"                                                       || ["12341"]
        "afterok:12341.my-pbs.example.com"                                           || ["12341"]
        "afterok:12341.server:13456.server"                                          || ["12341", "13456"]
        "afterok:12341.server:13456.server,beforeok:4356.server"                     || ["12341", "13456"]
        "asdfadf:9078.server,afterok:12341.server:13456.server,beforeok:4356.server" || ["12341", "13456"]
    }
}
