### Pre-requisites
- Java 8
- Maven 3.3.x
- ETCD (https://github.com/etcd-io/etcd)

### How to run
- Run ```mvn clean install ``` on the root source directory
- jar files for the master, worker and clients will be created in the ./bin/lib directory
- change the configuration file (.bin/conf/config.properties) file as required (change the input and output directories)
- Install and run ETCD Distributed key value store (https://github.com/etcd-io/etcd/releases/)
You can download the binary release and run ./etcd and it would startup an ETCD server
ex:
```wget https://github.com/etcd-io/etcd/releases/download/v3.3.12/etcd-v3.3.12-linux-amd64.tar.gz```

#### Using the binaries
You can run start and manage the setup using the given scripts in the ./bin directory. It provides the following scripts.
- start-cluster.sh
- stop-cluster.sh

Samples:
- start-sample-wordcount.sh
- start-sample-invertedIndex.sh
- start-sample-iterativeWordCount.sh

Please note that since the iterative sample runs through multiple iterations, it would take relatively long time to complete. 

For example, you can start the cluster with ```n``` number of workers, 
```
sh start-cluster.sh n
```

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
java -Dconfig.file=<config.properties_location> -jar iu.swithana.mapreduce.master.jar
```

##### Run Workers
```
java -jar iu.swithana.mapreduce.worker.jar
```

##### Run Samples
```
java -jar iu.swithana.mapreduce.sample.wordCount.jar
```
 