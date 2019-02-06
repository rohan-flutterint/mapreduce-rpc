### Pre-requisites
- Java 8
- Maven 3

### How to run
- Run ```mvn clean install ``` on the root source directory
- jar files for the master, worker and clients will be created in the ./bin/lib directory
- config files would be at .bin/conf directory 

#### Using the binaries
You can run start and manage the setup using the given scripts in the ./bin directory. It provides the following scripts.
- start-cluster.sh
- stop-cluster.sh

Samples:
- start-sample-wordcount.sh

For example, you can start the cluster with ```n``` number of workers, 
```sh start-cluster.sh n```

##### Logs
All the logs for the master and the workers will be created at the .bin/logs directory. 
If you need clean logs, please delete the logs in the logs directory before starting a new cluster. 

You can tail the logs using ```tail -f master.log``` command. 

#### Using the source
- All the jar files accepts optional property files as
``` -Dconfig.file=<config.properties>```. By default, it uses the bundled property file (resources/config.properties).
You can change any of the two ways to provide the property files. 

##### Run master
```
java -jar iu.swithana.mapreduce.master.jar
```

With the config file, 
```
java -jar iu.swithana.mapreduce.master.jar  -Dconfig.file=<config.properties>
```

##### Run Workers
```
java -jar iu.swithana.mapreduce.worker.jar
```

##### Run Samples
```
java -jar iu.swithana.mapreduce.sample.wordCount.jar
```

### Todo
- script to start a cluster
- iterative Map Reduce
- Add support for stragglers
- The submit job performs as an blockingRMI call. Need to remove that. 



#### Notes
- partitioning does not copy the keys, it's a view on the actual keyset using Guava
- ordered final result through tree map
- The remote map operation is a blocking call. Should be changed to async call and get a call back once the
 Mapper has finished the task
- support for stragglers: mappers only
 