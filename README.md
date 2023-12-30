# sbt 빌드 시
```
# maven scope를 compile로 해도 jar를 정상 로드하지 못 함
sbt package

# maven scope compile인 라이브러리를 로드하기 위해서는 아래로 빌드하여야 함
sbt assembly
```


# sbt 빌드 및 실행 방법
```
# 빌드
sbt clean package

# Spark 실행
${SPARK_HOME}/bin/spark-submit \
--class {클래스 정보} \
target/scala-2.12/scala-spark_2.12-1.0.jar 
```

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

# 사용 데이터   
## 1. 뉴욕 yellow taxi   
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## 2. 
