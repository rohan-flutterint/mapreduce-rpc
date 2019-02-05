### Pre-requisites
- Java 8
- Maven 3

### How to run
- All the jar files accepts optional property files as
``` -Dconfig.file=<config.properties>```. By default, it uses the bundled property file (resources/config.properties).
You can change any of the two ways to provide the property files. 
- Run ```mvn clean package ``` on the root source directory
- The jar files for the master, worker and clients will be created in the ./target directory


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
- iterative Map Reduce
- Add support for stragglers
- The submit job performs as an blockingRMI call. Need to remove that. 


#### Notes
- partitioning does not copy the keys, it's a view on the actual keyset using Guava
- ordered final result through tree map
- The remote map operation is a blocking call. Should be changed to async call and get a call back once the
 Mapper has finished the task
 