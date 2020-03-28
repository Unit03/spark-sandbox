```bash
docker-compose up namenode datanode hive-metastore-postgresql hive-metastore hive-server spark-master spark-worker
````

Use Python 2 :(

```bash
pip install pyspark
spark-submit --master spark://localhost:7077 hello.py
```
