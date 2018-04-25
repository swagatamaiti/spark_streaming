package com.mutliOrder.ETL.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.hive.HiveContext;

import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.ETL.constant.Constants;
import com.mutliOrder.common.Class.ClassUtils;
import com.mutliOrder.common.conf.ConfigFactory;


/**
 * spark工具
 *
 */
public class SparkUtils {
	
	public static void setAppName(SparkConf conf,String appName){
		conf.setAppName(appName);
	}
	
	public static void setEsConfig(SparkConf conf) throws FileNotFoundException{
		conf.set("es.nodes", ConfigFactory.getInstance()
				.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.ES_NODES))
		.set("pushdown", "true")
		.set("es.nodes.wan.only", "true")
		.set("es.http.timeout", "2m")
		.set("es.http.retries", "60")
		.set("es.batch.size.entries", "1000")
		.set("es.batch.write.refresh", "false")
		.set("es.batch.write.retry.count", "60")
		.set("es.batch.write.retry.wait", "60s")
		.set("mapreduce.job.user.classpath.first","true")
		.set("mapreduce.task.classpath.user.precedence","true")
		//设置密码后的参数
		/*.set("es.net.http.auth.user", ConfigFactory.getInstance()
				.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.ES_USER))
		.set("es.net.http.auth.pass", ConfigFactory.getInstance()
				.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.ES_PASSWORD))*/;
	}
	
	public static void setStreamingTtl(SparkConf conf) throws Exception {
		setConf(conf);
		conf.set("spark.cleaner.ttl", ConfigFactory.getInstance()
				.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_CLEANER_TTL))
			.set("spark.streaming.backpressure.enabled", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_PRESSURE_ENABLED))
			.set("spark.streaming.unpersist", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_STREAMING_UNPERSIST))
			.set("spark.executor.extraJavaOptions", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS))
			.set("spark.streaming.kafka.maxRatePerPartition", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_KAFKA_MAXRATPERPARTITON))
			.set("spark.streaming.backpressure.initialRate", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_STREAMING_BACKPRESS_INITIALRATE))
			.set("spark.streaming.stopGracefullyOnShutdown", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_STREAMING_STOPGRACEFULLY));
	}
	
	public static void setSyncDataConf(SparkConf conf) throws Exception {
		setConf(conf);
		conf.set("spark.executor.memory", "8g")
			.set("spark.executor.cores", "1")
			.set("spark.default.parallelism", "10");
	}
	/**
	 * 设置spark配置参数
	 * 对应firdata.properties中spark开头参数
	 * @param conf
	 * @throws IOException 
	 */
	public static void setConf(SparkConf conf) throws IOException {
		String beanPackage = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
				.getProperty(Constants.BEAN_PACKAGE);
		Map<String, Class<?>> map = ClassUtils.getClassesExtends(beanPackage, Model_Base.class);
		Iterator<Entry<String, Class<?>>> it = map.entrySet().iterator();
		int k = map.size()+1;
		Class<?>[] clases = new Class[k];
		int i = 0;
		while(it.hasNext()){
			Entry<String, Class<?>> entry = it.next();
			clases[i] = entry.getValue();
			i++;
		}
		clases[i] = Model_Base.class;
			conf.setAppName(ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_APP_NAME))
/*			.set("spark.shuffle.manager",ConfigFactory.getInstance()
					.getConfigProperties(ConfigFactory.DATA_CONFIG_PATH).getProperty(Constants.SPARK_SHUFFLE_MANAGER))*/
			.set("spark.shuffle.sort.bypassMergeThreshold",ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_SHUFFLE_SORT_BYPASSMERGETHRESHOLD))
/*			.set("spark.default.parallelism", ConfigFactory.getInstance()
					.getConfigProperties(ConfigFactory.DATA_CONFIG_PATH).getProperty(Constants.SPARK_DEFAULT_PARALLELISM))*/
			.set("spark.local.dir",ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_LOCAL_DIR))
			.set("spark.storage.memoryFraction", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_STORAGE_MEMORYFRACTION))  
			.set("spark.shuffle.consolidateFiles", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_SHUFFLE_CONSOLIDATEFILES))
			.set("spark.shuffle.file.buffer", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_SHUFFLE_FILE_BUFFER))  
			.set("spark.shuffle.memoryFraction", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_SHUFFLE_MEMORYFRACTION))    
			.set("spark.reducer.maxSizeInFlight", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_REDUCE_MAXSIZEINFLIGHT))  
			.set("spark.shuffle.io.maxRetries", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_SHUFFLE_IO_MAXRETRIES))  
			.set("spark.shuffle.io.retryWait", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_SHUFFLE_IO_RETRYWAIT))
			.set("spark.streaming.blockInterval", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_STREAMING_BLOCKINTERVAL))
			.set("spark.network.timeout", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_NETWORK_TIMEOUT))
			.set("spark.kryoserializer.buffer.max",ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_KRYOSERIALIZER_BUFFER_MAX))
			.set("spark.serializer", ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SPARK_SERIALIZER))
//			.set("spark.default.parallelism", "50")
			.registerKryoClasses(clases);
		
	}
	/**
	 * 设置hive配置
	 * @param hiveContext
	 * @throws FileNotFoundException
	 */
	public static void setHiveContext(HiveContext hiveContext) throws FileNotFoundException {
		
		String hiveSql = "set " + Constants.HIVE_EXEC_DYNAMIC_PARTITION + "=" + ConfigFactory.getInstance()
				.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.HIVE_EXEC_DYNAMIC_PARTITION);
		hiveContext.sql(hiveSql);
		
		hiveSql = "set " + Constants.HIVE_EXEC_DYNAMIC_PARTITION_MODE + "=" + ConfigFactory.getInstance()
				.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.HIVE_EXEC_DYNAMIC_PARTITION_MODE);
		hiveContext.sql(hiveSql);
		
	   hiveSql = "set " + Constants.HIVE_EXEC_MAX_DYNAMIC_PARTITION_PERNODE + "=" + ConfigFactory.getInstance()
				.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.HIVE_EXEC_MAX_DYNAMIC_PARTITION_PERNODE);
	   hiveContext.sql(hiveSql);
	   
	   hiveContext.sql("set spark.sql.crossJoin.enabled = true");
		
	}

}