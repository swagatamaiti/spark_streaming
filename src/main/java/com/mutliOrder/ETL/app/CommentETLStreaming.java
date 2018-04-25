package com.mutliOrder.ETL.app;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.mutliOrder.DataBean.Clean;
import com.mutliOrder.DataBean.bean.Model_CommentContentList;
import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.ETL.app.worker.CommentWorker;
import com.mutliOrder.ETL.constant.Constants;
import com.mutliOrder.ETL.helper.HbaseHelper;
import com.mutliOrder.ETL.helper.HttpRequest;
import com.mutliOrder.ETL.util.GenUuid;
import com.mutliOrder.ETL.util.SparkUtils;
import com.mutliOrder.common.Time.TimeUtils;
import com.mutliOrder.common.conf.ConfProperties;
import com.mutliOrder.common.conf.ConfigFactory;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class CommentETLStreaming {
	
	private static Logger logger;
	private static String checkptPath = null;
	private static String family = "cf";
	private static String DataflowMonitorTable = "DataFlowMonitor";
	/**
	 * 日志，property配置文件配置
	 */
	static {
		ConfProperties configProp = null;
		try {
			configProp = ConfigFactory.getInstance().getConfigProperties(Constants.LOG4J_CONFIG_PATH);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		PropertyConfigurator.configure(configProp);
		logger = Logger.getLogger(CommentETLStreaming.class);
		try {
			checkptPath = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
					.getProperty(Constants.CHECKPOINT_PATH_FOR_COMMENT);
			family = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
					.getProperty(Constants.FAMILY);
			DataflowMonitorTable = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
					.getProperty(Constants.HBASE_DATAFLOW_MONITOR_TABLE);
		} catch (FileNotFoundException e) {

			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws Exception {
		logger.info("DataEtlStreaming start work--------------version 11/14 12:01");

		JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(checkptPath, new JavaStreamingContextFactory() {
			public JavaStreamingContext create() {
				try {
					return createContext();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}
		});

		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}

	public static JavaStreamingContext createContext() throws Exception {

		SparkConf conf = new SparkConf();

		SparkUtils.setConf(conf);
//		SparkUtils.setEsConfig(conf);
		SparkUtils.setAppName(conf, "CommentETLStreaming");
		SparkUtils.setStreamingTtl(conf);
		// 设置spark任务时间间隔，默认15分钟
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(ConfigFactory.getInstance()
				.getConfigProperties(Constants.DATA_CONFIG_PATH).getInt(Constants.BATCH_INTEL, 15)));
		// do checkpoint metadata to hdfs

		jssc.checkpoint(checkptPath);

		// 从kafka持续获取数据
		Map<String, String> kafkaParams = new HashMap<String, String>();

		String kafkaBrokenList = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
				.getProperty(Constants.KAFKA_BROKEN_LIST);
		kafkaParams.put("metadata.broker.list", kafkaBrokenList);

		// 获取kafka的主题
		String kafkaTopics = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
				.getProperty(Constants.KAFKA_TOPICS_FOR_COMMENT);
		logger.info("start consumer messages of topic : " + kafkaTopics);

		String[] kafkaTopicsSplited = kafkaTopics.split(",");

		Set<String> topics = new HashSet<String>();
		for (String kafkaTopic : kafkaTopicsSplited) {
			topics.add(kafkaTopic);
		}

		JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		// 对rdd做转换操作，将k-v转换为String
		JavaDStream<String> records = lines.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public JavaRDD<String> call(JavaPairRDD<String, String> v1) throws Exception {

				JavaRDD<String> record = v1.map(new Function<Tuple2<String, String>, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple2<String, String> tuple) throws Exception {
						return tuple._2;
					}
				});
				return record;
			}
		});
		
		records.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> Rdd) throws Exception {
				if(!Rdd.isEmpty()){
					JavaRDD<Model_Base> modelRdd = Rdd.map(new Function<String, Model_Base>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Model_Base call(String arg0) throws Exception {
							// TODO Auto-generated method stub
							return Clean.process(arg0);
						}
					}).filter(new Function<Model_Base, Boolean>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(Model_Base arg0) throws Exception {
							// TODO Auto-generated method stub
							return arg0!=null&&Model_CommentContentList.class.equals(arg0.getClass());
						}
					}).persist(StorageLevel.DISK_ONLY());
					long l = modelRdd.count();
					
					HttpRequest.sendGet("http://192.168.1.55:8989/api/bigdata/setstatus/", "dataName=Model_CommentContentList&count="+l);
					Map<String, Long> map = new HashMap<String, Long>();
					map.put(Model_CommentContentList.class.getSimpleName(), l);
					ETLStatics(map);
					
					JavaRDD<Model_Base> pairRdd = modelRdd.flatMap(new FlatMapFunction<Model_Base, Model_Base>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Iterable<Model_Base> call(Model_Base arg0) throws Exception {
							// TODO Auto-generated method stub
							return arg0.split();
						}
					}).filter(new Function<Model_Base, Boolean>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(Model_Base mb) throws Exception {
							return mb != null;
						}
					});
					CommentWorker.run(pairRdd);
					modelRdd.unpersist();
				}
			}
		});

		return jssc;
	}
	
	private static void ETLStatics(Map<String, Long> classCountMap) {
		HashMap<String, String> clMap = new HashMap<String, String>();
		clMap.put("time", TimeUtils.getNowYMDHMS().substring(0, 10));
		clMap.put("count", classCountMap.toString());
		try {
			HbaseHelper.getInstance().insert(DataflowMonitorTable, GenUuid.GetUuid(), family, clMap);
		} catch (IOException e) {
			logger.error(e);
		}
	}


}
