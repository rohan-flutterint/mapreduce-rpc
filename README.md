### How to run
- All the jar files accepts optional property files as
``` -Dconfig.file=<config.properties>```. By default, it uses the bundled property file (resources/config.properties).
You can change any of the two ways to provide the property files. 
- Run ```maven package``` on the root source directory
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
- Partitioning of the key set of the map output
- iterative Map Reduce
- The remote map operation is a blocking call. Should be changed to async call and get a call back once the
 Mapper has finished the task
