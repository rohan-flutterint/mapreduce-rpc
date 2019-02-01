### How to run
- Run ```maven package``` on the root source directory
- The jar files for the master, worker and clients will be created in the ./target directory
- All the jar files accepts optional property files as
``` -Dconfig.file=<config.properties>```
- By default, it uses the bundled property file (resources/config.properties)

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
