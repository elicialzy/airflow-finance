# 3107-pipeline

# NEEDED: Install mtools
Install mtools [http://blog.rueckstiess.com/mtools/mlaunch.html]. mtools is a compiled list of scripts useful for managing local mongoDB.
- Refer to docs [http://blog.rueckstiess.com/mtools/mlaunch.html]

Transactions in mongoDB are only allowed on a replica set members. To make life easier, we will use mlaunch to start the required instances. 

1. Go to 3107-pipeline folder. Run `mlaunch --replicaset`
2. Type `mlaunch list`, you should see the following, with the following port numbers. 
```
    PROCESS    PORT     STATUS     PID

    mongod     27017    running    3635
    mongod     27018    running    3710
    mongod     27019    running    3781
```
3. It will generate a data folder in the dir where you ran the command. The data folder contains all the data for the mongoDB instances e.g. indexes, collections, change logs. You should see data > replset > [rs1, rs2, rs3]
4. Subsequently, you can go to the dir where the data folder is and type `mlaunch start` to start the local mongod instance to run replica sets

# NEEDED: Directories
1. Custom python files created should be placed in the `3107-pipeline/dags` dir to avoid importing issues in DAG files.

# NEEDED: Installing gcloud CLI 

Follow: https://cloud.google.com/sdk/docs/install#deb

```
sudo apt-get install apt-transport-https ca-certificates gnupg

echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

sudo apt-get update && sudo apt-get install google-cloud-cli

gcloud init --console-only
```

Project ID
```
project-346314
```

GCS bucket
```
gs://3107-cloud-storage/
```

Example commands to store json to GCS, then load to BQ
```
gsutil cp restaurant.json gs://3107-cloud-storage/restaurant.json
```

With schema
```
bq load --project_id=project-346314  \
    --autodetect=true \
    --source_format=NEWLINE_DELIMITED_JSON  \
    3107.Restaurant \
    gs://3107-cloud-storage/restaurant.json
```

Pointers to note

1. Defined schema in the following manner, refer to static/restaurantSchema.json for example
```
[
 {
   "description": "[DESCRIPTION]",
   "name": "[NAME]",
   "type": "[TYPE]",
   "mode": "[MODE]"
 },
 {
   "description": "[DESCRIPTION]",
   "name": "[NAME]",
   "type": "[TYPE]",
   "mode": "[MODE]"
 }
]
```

2. Do not wrap json objects in array, BQ does not recognize arrays out of an object, refer to static/restaurant.json for example
```
{"name": ".CN Chinese","URL": "http://www.just-eat.co.uk/restaurants-cn-chinese-cardiff/menu","outcode": "CF24","postcode": "3JH","address": "228 City Road","type_of_food": "Chinese"}
{"name": ".CN Chinese","URL": "http://www.just-eat.co.uk/restaurants-cn-chinese-cardiff/menu","outcode": "CF24","postcode": "3JH","address": "228 City Road","type_of_food": "Chinese"}
{"name": ".CN Chinese","URL": "http://www.just-eat.co.uk/restaurants-cn-chinese-cardiff/menu","outcode": "CF24","postcode": "3JH","address": "228 City Road","type_of_food": "Chinese"}
```

Following is not allowed
```
[
    {"name": ".CN Chinese","URL": "http://www.just-eat.co.uk/restaurants-cn-chinese-cardiff/menu","outcode": "CF24","postcode": "3JH","address": "228 City Road","type_of_food": "Chinese"}
    {"name": ".CN Chinese","URL": "http://www.just-eat.co.uk/restaurants-cn-chinese-cardiff/menu","outcode": "CF24","postcode": "3JH","address": "228 City Road","type_of_food": "Chinese"}
    {"name": ".CN Chinese","URL": "http://www.just-eat.co.uk/restaurants-cn-chinese-cardiff/menu","outcode": "CF24","postcode": "3JH","address": "228 City Road","type_of_food": "Chinese"}
]
```

3. Sample query using bq via command line. Note the flag

```
bq query --use_legacy_sql=false \
'SELECT
  *
FROM
  `project-346314`.3107.Restaurant'
```

# NEEDED: Installing jq
```
sudo apt  install jq
```

# NOT NEEDED: Install mongo-bigquery (which depends on bq and gsutil from gcloud CLI)

Follow: https://github.com/gotitinc/mongo-bigquery

You need to install maven for this via the apt
```
sudo apt install maven
```

Change following in onefold.py

```
TMP_PATH = '/home/airflow/tmp/onefold_mongo'
CLOUD_STORAGE_PATH = 'gs://3107-bucket'
HADOOP_MAPREDUCE_STREAMING_LIB = "/home/airflow/hadoop-3.3.2/share/hadoop/tools/lib/hadoop-streaming-3.3.2.jar"
ONEFOLD_MAPREDUCE_JAR = "/home/airflow/mongo-bigquery/java/MapReduce/target/MapReduce-0.0.1-SNAPSHOT.jar"
ONEFOLD_HIVESERDES_JAR = "/home/airflow/mongo-bigquery/java/HiveSerdes/target/hive-serdes-1.0-SNAPSHOT-shaded.jar"
```

# NOT NEEDED: Hadoop

Following mainly https://phoenixnap.com/kb/install-hadoop-ubuntu BUT FOLLOW COMMANDS before as i fixed some non-working commands

1. install openJDK
```
sudo apt update
sudo apt install openjdk-8-jdk -y
```

2. install open ssh and create passwordless SSH
```
sudo apt install openssh-server openssh-client -y
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ssh localhost
```
3. download and install haddop
```
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.2/hadoop-3.3.2.tar.gz
tar xzf hadoop-3.3.2.tar.gz
sudo nano .bashrc
```

alternatively, copy paste the hadoop-3.3.2.tar.gz from static to ur root folder, and run 

```
tar xzf hadoop-3.3.2.tar.gz
```

4. configure path in bashrc for hadoop
```
sudo vim .bashrc

export HADOOP_HOME="/home/airflow/hadoop-3.3.2"
export HADOOP_INSTALL="$HADOOP_HOME"
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export HADOOP_COMMON_HOME="$HADOOP_HOME"
export HADOOP_HDFS_HOME="$HADOOP_HOME"
export YARN_HOME="$HADOOP_HOME"
export HADOOP_COMMON_LIB_NATIVE_DIR="$HADOOP_HOME/lib/native"
export PATH="$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin"
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/nativ"

source ~/.bashrc
```
5. Edit hadoop-env.sh File
```
sudo vim $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```
add this path
```
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```
6. Edit core-site.xml File
```
sudo vim $HADOOP_HOME/etc/hadoop/core-site.xml

<configuration>
<property>
<name>hadoop.tmp.dir</name>
<value>/home/airflow/hdoop/tmpdata</value>
</property>
<property>
<name>fs.default.name</name>
<value>hdfs://127.0.0.1:9000</value>
</property>
</configuration>
```
Remember to create the required folder at this path /home/airflow/hdoop/tmpdata

7. Edit hdfs-site.xml File
```
sudo vim $HADOOP_HOME/etc/hadoop/hdfs-site.xml

<configuration>
<property>
<name>dfs.data.dir</name>
<value>/home/airflow/hdoop/dfsdata/namenode</value>
</property>
<property>
<name>dfs.data.dir</name>
<value>/home/airflow/hdoop/dfsdata/datanode</value>
</property>
<property>
<name>dfs.replication</name>
<value>1</value>
</property>
</configuration>
```
Remember to create the required folder at this path /home/airflow/hdoop/dfsdata

8. Edit mapred-site.xml File
```
sudo vim $HADOOP_HOME/etc/hadoop/mapred-site.xml

<configuration>
<property>
<name>mapreduce.framework.name</name>
<value>yarn</value>
</property>
</configuration>
```
9. Edit yarn-site.xml File
```
sudo vim $HADOOP_HOME/etc/hadoop/yarn-site.xml

<configuration>
<property>
<name>yarn.nodemanager.aux-services</name>
<value>mapreduce_shuffle</value>
</property>
<property>
<name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
<value>org.apache.hadoop.mapred.ShuffleHandler</value>
</property>
<property>
<name>yarn.resourcemanager.hostname</name>
<value>127.0.0.1</value>
</property>
<property>
<name>yarn.acl.enable</name>
<value>0</value>
</property>
<property>
<name>yarn.nodemanager.env-whitelist</name>   
<value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PERPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
</property>
</configuration>
```
10. Format HDFS NameNode
```
hdfs namenode -format
```
11. Start cluster, wait until each step terminates
```
cd hadoop-3.3.2/sbin

./start-dfs.sh
./start-yarn.sh
```
checking whether the expected Hadoop processes are running 
```
jps
```

12. Add port forwarding
settings > network > advanced > port forwarding

Hadoop NameNode UI: 9870
access individual DataNodes: 9864
YARN Resource Manager: 8088

http://localhost:9870
http://localhost:9864
http://localhost:8088

You can also check with netstat if Hadoop is listening on the configured ports.
```
sudo netstat -plten | grep java
```

stop hadoop
./hadoop-3.3.2/sbin/stop-all.sh

start hadoop
./hadoop-3.3.2/sbin/start-all.sh
