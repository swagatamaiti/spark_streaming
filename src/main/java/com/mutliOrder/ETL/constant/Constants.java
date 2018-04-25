package com.mutliOrder.ETL.constant;

public interface Constants {
	/******************************** 特殊字符 ********************************/
	String SPILT_FLAG = "#!@";
	String BLANK=" ";
	String UNDER_LINE="_";
	String STAR="*";
	/******************************** zookeeper配置字段 ********************************/
	String ZK_LIST = "zk.list";
	String MAX_QUERY_LIMIT = "max.query.limit";
	/********************************离线清洗字段 ********************************/
	/**离线清洗中需要上传到在线数据库的类**/
	String CLASS_INFO = "class.info";
	/******************************** spark配置字段 ********************************/
	/** spark程序名**/
	String SPARK_APP_NAME = "spark.app.name";
	/** spark本地目录**/
	String SPARK_LOCAL_DIR = "spark.local.dir";
	String SPARK_SHUFFLE_MANAGER = "spark.shuffle.manager";
	String SPARK_SHUFFLE_SORT_BYPASSMERGETHRESHOLD = "spark.shuffle.sort.bypassMergeThreshold";
	String SPARK_DEFAULT_PARALLELISM = "spark.default.parallelism";
	String SPARK_STORAGE_MEMORYFRACTION = "spark.storage.memoryFraction";
	String SPARK_SHUFFLE_CONSOLIDATEFILES= "spark.shuffle.consolidateFiles";
	String SPARK_SHUFFLE_FILE_BUFFER = "spark.shuffle.file.buffer";
	String SPARK_SHUFFLE_MEMORYFRACTION = "spark.shuffle.memoryFraction";
	String SPARK_REDUCE_MAXSIZEINFLIGHT = "spark.reducer.maxSizeInFlight";
	String SPARK_SHUFFLE_IO_MAXRETRIES = "spark.shuffle.io.maxRetries";
	String SPARK_SHUFFLE_IO_RETRYWAIT = "spark.shuffle.io.retryWait";
	String SPARK_SERIALIZER = "spark.serializer";
	String SPARK_STREAMING_BLOCKINTERVAL = "spark.streaming.blockInterval";
	String SPARK_NETWORK_TIMEOUT = "spark.network.timeout";
	String SPARK_CLEANER_TTL = "spark.cleaner.ttl";
	String SPARK_PRESSURE_ENABLED = "pressure.enabled";
	String SPARK_STREAMING_UNPERSIST = "spark.streaming.unpersist";
	String SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS = "spark.executor.extraJavaOptions";
	String SPARK_STREAMING_STOPGRACEFULLY = "spark.streaming.stopGracefullyOnShutdown";
	String SPARK_KRYOSERIALIZER_BUFFER_MAX="spark.kryoserializer.buffer.max";
	String SPARK_KAFKA_MAXRATPERPARTITON = "spark.streaming.kafka.maxRatePerPartition";
	
	String SOLR_ZK_HOST = "solr.zk.host";
	
	String HIVE_EXEC_DYNAMIC_PARTITION = "hive.exec.dynamic.partition";
	String HIVE_EXEC_DYNAMIC_PARTITION_MODE = "hive.exec.dynamic.partition.mode";
	String HIVE_EXEC_MAX_DYNAMIC_PARTITION_PERNODE = "hive.exec.max.dynamic.partitions.pernode";
	
	String FAMILY = "hbase.fire.cf";
	String INDEX="es.index";
	
	String BATCH_INTEL = "batch.intel";
	String CHECKPOINT_PATH = "checkpoint.path";
	String CHECKPOINT_PATH_FOR_COMMENT = "checkpoint.path.for.comment";
	
	String KAFKA_BROKEN_LIST = "kafka.broken.list";
	String KAFKA_TOPICS = "kafka.topics";
	String KAFKA_TOPICS_FOR_COMMENT = "kafka.topics.for.comment";
	String KAFKA_GROUPID = "kafka.groupid";
	
	String REPAIR_CLASS="repair.class";
	
	String ES_NODES="es.nodes";
	String ES_URL = "es.url";
	String ES_USER = "es.user";
	String ES_PASSWORD = "es.password";
	
	
	String DATA_CONFIG_PATH = "/chaole.properties";
	String LOG4J_CONFIG_PATH = "/log.properties";
	
	String COLLECTION="collection";
	String TABLE="table";
	String DEL_SON= "delson";
	String DEL_UNIQUE_FIELDS="delUniqueFields";
	String UNIQUE_FIELDS="uniqueFields";
	
	String PUT_ACTION = "put";
	String DELETE_ACTION = "delete";
	
	String BEAN_PACKAGE = "bean.package";
	
	String BATCH_FLAG="batch.flag";
	String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	
	
	String HBASE_DATAFLOW_MONITOR_TABLE ="hbase.dataflow.monitor.table";
	
	String NEED_FIELDS = "need.fields";
	String SPARK_STREAMING_BACKPRESS_INITIALRATE = "spark.streaming.backpressure.initialRate";
	
}
