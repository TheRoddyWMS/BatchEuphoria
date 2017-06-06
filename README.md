# BatchEuphoria
A library for cluster / batch system developers to create batch jobs from Java without any hassle and drama.

### Dependencies
* [RoddyToolLib](https://github.com/eilslabs/RoddyToolLib)

## How to use it
Use `gradle build` to create the jar file.

Currently the library only supports PBS, LSF REST and direct execution.

First you need to create an execution service depending on the kind of cluster you have.

For LSF REST you need to use the RestExecutionService:

`RestExecutionService executionService = new RestExecutionService("http://yourServer:8080/platform/ws","account","password")`

For PBS you need to implement your own execution service with the `ExecutionService interface`

`JobManagerCreationParameters parameters = new JobManagerCreationParametersBuilder().build()`

Currently there are two job managers which are `LSFRestJobManager` and `PBSJobManager`.
For example for LSF you would initialize the job manager like this:

`LSFRestJobManager jobManager = new LSFRestJobManager(executionService,parameters)`

You need a resource set to define your requirements like how many cores and how much memory and the time limit you need for your job. 

`ResourceSet resourceSet = new ResourceSet(ResourceSetSize.s, new BufferValue(10, BufferUnit.m), 1, 1, new TimeUnit("m"), null, null, null)`

Then you create the Job with job name, submission script, resource set, environment variables etc.

```
String script=[ "#!/bin/bash", "sleep 15" ].join("\n")`
BEJob testJobwithScript = new BEJob("batchEuphoriaTestJob", null, script, null, resourceSet, null, ["a": "value"], null, null, jobManager)`
```

**NOTE** Submitted jobs are in HOLD state by default! You need to call startHeldJobs on your job manager instance at the end. Or, if you need it, cancel them e.g. on an error.


All job managers support the following functions:

- Submit job: `jobManager.runJob(job)`

- Abort job: `jobManager.queryJobAbortion(jobList)`

- Start held jobs: `jobManager.startHeldJobs(jobList)`




## Integration Tests
Use `gradle uberJar` to setup the jar file for integration tests.

To test it with LSF environment use `java -jar BatchEuphoria.jar -s yourServer -a yourAccount -rs http://yourServer:8080/platform/ws -ra yourRestAccount -c lsf`

For PBS environment use `java -jar BatchEuphoria.jar -s yourServer -a yourAccount -c pbs`
