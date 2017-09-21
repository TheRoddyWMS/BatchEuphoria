# BatchEuphoria
[![Build Status - Travis](https://travis-ci.org/eilslabs/BatchEuphoria.svg?branch=develop)](https://travis-ci.org/eilslabs/BatchEuphoria)

A library for cluster / batch system developers to create batch jobs from Java without any hassle and drama.
Currently this library supports the following job schedulers:
* PBS
* LSF (REST)
* direct execution

### Dependencies
* [RoddyToolLib](https://github.com/eilslabs/RoddyToolLib)

## Build

Building is as simple as

```bash
./gradlew build
```

If you are behind a firewall and need to access the internet via a proxy, you can configure the proxy in `$HOME/.gradle/gradle.properties`:

```groovy
systemProp.http.proxyHost=HTTP_proxy
systemProp.http.proxyPort=HTTP_proxy_port
systemProp.https.proxyHost=HTTPS proxy
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
Use `gradle uberJar` to setup the jar file for integration tests.

To test it with LSF environment use `java -jar BatchEuphoria.jar -s yourServer -a yourAccount -rs http://yourServer:8080/platform/ws -ra yourRestAccount -c lsf`

For PBS environment use `java -jar BatchEuphoria.jar -s yourServer -a yourAccount -c pbs`
