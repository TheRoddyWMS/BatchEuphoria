[![CircleCI](https://circleci.com/gh/TheRoddyWMS/BatchEuphoria/tree/master.svg?style=svg)](https://circleci.com/gh/TheRoddyWMS/BatchEuphoria/tree/master) [![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FTheRoddyWMS%2FBatchEuphoria.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2FTheRoddyWMS%2FBatchEuphoria?ref=badge_shield)

# BatchEuphoria

A library for cluster / batch system developers to create batch jobs from Java without any hassle and drama.
Currently this library supports the following job schedulers:
* PBS (SSH+CLI)
* LSF (SSH+CLI and REST) Version 10.1 Fix Pack 2 or later
* SLURM (SSH+CLI)
* direct execution

### Dependencies
* [RoddyToolLib](https://github.com/TheRoddyWMS/RoddyToolLib)

## Build

Building is as simple as

```bash
./gradlew build
```

If you are behind a firewall and need to access the internet via a proxy, you can configure the proxy in `$HOME/.gradle/gradle.properties`:

```groovy
systemProp.http.proxyHost=HTTP_proxy
systemProp.http.proxyPort=HTTP_proxy_port
systemProp.https.proxyHost=HTTPS_proxy
systemProp.https.proxyPort=HTTPS_proxy_port
```

where you substitute the correct proxies and ports required for your environment.

## How to use it

First you need to create an execution service depending on the kind of job scheduler you have.

For LSF REST you need to use the RestExecutionService:

```groovy
RestExecutionService executionService = new RestExecutionService("http://yourServer:8080/platform/ws","account","password")
```

For PBS you need to implement your own execution service with the `ExecutionService interface`

Currently there are two job managers which are `LSFRestJobManager` and `PBSJobManager`.
For example for LSF you would initialize the job manager like this:

```groovy
JobManagerCreationParameters parameters = new JobManagerCreationParametersBuilder().build()
LSFRestJobManager jobManager = new LSFRestJobManager(executionService,parameters)
```
For PBS it looks like this:
```groovy
JobManagerCreationParameters parameters = new JobManagerCreationParametersBuilder().build()
PBSJobManager jobManager = new PBSJobManager(executionService,parameters)
```

You need a resource set to define your requirements like how many cores and how much memory and the time limit you need for your job. 

```groovy
ResourceSet resourceSet = new ResourceSet(ResourceSetSize.s, new BufferValue(10, BufferUnit.m), 1, 1, new TimeUnit("m"), null, null, null)
```

Then you create the Job with job name, submission script, resource set, environment variables etc.

```groovy
String script=[ "#!/bin/bash", "sleep 15" ].join("\n")`
BEJob testJobwithScript = new BEJob("batchEuphoriaTestJob", null, script, null, resourceSet, null, ["a": "value"], null, null, jobManager)`
```

**NOTE** Submitted jobs are in HOLD state by default! You need to call startHeldJobs on your job manager instance at the end. Or, if you need it, cancel them e.g. on an error.


All job managers support the following functions:

- Submit job: `jobManager.runJob(testJobwithScript)` For PBS the submitted jobs are set on hold by default.

- Abort job: `jobManager.queryJobAbortion([testJobwithScript])`

- Start held jobs: `jobManager.startHeldJobs([testJobwithScript])`

You can find [here](https://github.com/eilslabs/BatchEuphoria/blob/develop/src/main/groovy/de/dkfz/roddy/BEIntegrationTestStarter.groovy) the integration tests with full example code for PBS and LSF.


## Integration Tests

To start the integration tests, please fill in host and user settings (password for lsf rest) into integrationTest.properties. Then start the tests like you'd start.

# Change Logs

* 0.2.0
  - major: CommandI classes to model commands and code
  - minor: ApptainerCommmandBuilder to create a wrapping command to run code in containers
  - patch: Library updates

* 0.1.4
  - patch: Changed the conversion of EpochSeconds to ZonedDateTime for the value "0".
    * For eligibleTime it is set to null
    * For startTime it is set to endTime

* 0.1.3
  - patch: Added getter for JobState enum.
  - patch: Fix SLURM API.
    * Reworked queryExtendedJobStateById to always execute both scontrol and sacct and combine the output
    * Some refactoring and added comments
    * Fixed time parsing for jobs longer than 24h
    * Added --propagate=none" to getAdditionalCommandParameters
    * Added tests

* 0.1.2
  - patch: Fix SLURM API.
    * Always return an asked resource for SLURM with requested memory, cores, and nodes.
    * Correctly return "NodeList" value as `JobInfo.executionHosts`. WARNING: The returned value is just the string returned from SLURM. No splitting of a list of nodes is yet implemented.
    * Fix "NumNodes" parsing (seems to return "-"-split value and last value is the number of nodes) and deal with "Unkown" times.
    * Fixed time parsing.

* 0.1.1
   - Patch: Fix: A newer LSF release is pickier on the position of the `-o` parameter in `bjobs`. Adapted to this upstream change. Probably code will still work with older LSF versions.
   - Patch: Bumped from RoddyToolLib 2.1.1 to 2.2.2.

* 0.1.0
   - Minor: SLURM support

* 0.0.14

   - Minor: Added execution time out feature
   - Patch: Update to RoddyToolLib 2.4.0 for better handling of stdout/stderr in ExecutionResult
   - Patch: Switched from Travis-CI to Circle-CI

* 0.0.13

   - Update to RoddyToolLib 2.3.0 (\[Async\]ExecutionResult). Explicitly use stdout instead of `ExecutionResult.resultLines` that could also contain stderr, dependent on execution service.

* 0.0.12

   - accounting name added to Job and implementation of `-P` parameter for `bsub` (LSF).
   - fixed year-inference unit test

* 0.0.7-1 (Release branch 0.0.7)
  - Patch: Fix: A newer LSF release is pickier on the position of the `-o` parameter in `bjobs`. Adapted to this upstream change. Probably code will still work with older LSF versions.

* 0.0.7