# Spark StandAlone Mode 구축 방법

spark download file 불러옴

${SPARK_HOME}/conf/spark-defaults.conf 파일 수정
```
spark.master                     spark://localhost:7077
spark.serializer                 org.apache.spark.serializer.KryoSerializer
```

${SPARK_HOME}/conf/spark-env.sh 파일 수정
```
SPARK_MASTER_HOST=localhost
```

${SPARK_HOME}/conf/workers 파일 수정
```
localhost
```

시작
```
sbin/start-master.sh

sbin/start-worker.sh

```
