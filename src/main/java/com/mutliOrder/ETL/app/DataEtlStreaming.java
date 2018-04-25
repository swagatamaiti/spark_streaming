package com.mutliOrder.ETL.app;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import com.mutliOrder.DataBean.Clean;
import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.ETL.app.worker.ETLWorker;
import com.mutliOrder.ETL.constant.Constants;
import com.mutliOrder.ETL.helper.HbaseHelper;
import com.mutliOrder.ETL.helper.HttpRequest;
import com.mutliOrder.ETL.util.GenUuid;
import com.mutliOrder.ETL.util.SparkUtils;
import com.mutliOrder.ETL.util.ToolUtils;
import com.mutliOrder.common.Class.ClassUtils;
import com.mutliOrder.common.Time.TimeUtils;
import com.mutliOrder.common.conf.ConfProperties;
import com.mutliOrder.common.conf.ConfigFactory;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

public class DataEtlStreaming {

	private static Logger logger;
	// private static String checkptPath = null;
	private static String beanPackage = null;
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
		logger = Logger.getLogger(DataEtlStreaming.class);
		try {
			// checkptPath =
			// ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
			// .getProperty(Constants.CHECKPOINT_PATH);
			beanPackage = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
					.getProperty(Constants.BEAN_PACKAGE);
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
		JavaStreamingContext jssc = createContext();
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}

	public static JavaStreamingContext createContext() throws Exception {

		SparkConf conf = new SparkConf();

		SparkUtils.setConf(conf);
		// SparkUtils.setEsConfig(conf);
		SparkUtils.setAppName(conf, "DataEtlStreaming");
		SparkUtils.setStreamingTtl(conf);
		// 设置spark任务时间间隔，默认15分钟

		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(ConfigFactory.getInstance()
				.getConfigProperties(Constants.DATA_CONFIG_PATH).getInt(Constants.BATCH_INTEL, 15)));
		final String group_id = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
				.getProperty(Constants.KAFKA_GROUPID);
		// do checkpoint metadata to hdfs 改成自己维护，故不用checkpoint
		// jssc.checkpoint(checkptPath);

		// 从kafka持续获取数据
		Map<String, String> kafkaParams = new HashMap<String, String>();

		String kafkaBrokenList = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
				.getProperty(Constants.KAFKA_BROKEN_LIST);
		kafkaParams.put("metadata.broker.list", kafkaBrokenList);
		kafkaParams.put("group.id", group_id);

		// 获取kafka的主题
		String kafkaTopics = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
				.getProperty(Constants.KAFKA_TOPICS);
		logger.info("start consumer messages of topic : " + kafkaTopics);

		String[] kafkaTopicsSplited = kafkaTopics.split(",");

		Set<String> topics = new HashSet<String>();
		for (String kafkaTopic : kafkaTopicsSplited) {
			topics.add(kafkaTopic);
		}
		java.util.Map<kafka.common.TopicAndPartition, Long> fromOffsets = new java.util.HashMap<kafka.common.TopicAndPartition, Long>();
		scala.collection.mutable.Map<String, String> mutableKafkaParam = JavaConversions.mapAsScalaMap(kafkaParams);
		scala.collection.immutable.Map<String, String> immutableKafkaParam = mutableKafkaParam
				.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> apply(Tuple2<String, String> v1) {
						return v1;
					}
				});
		KafkaCluster kafkaCluster = new KafkaCluster(immutableKafkaParam);
		scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topics);
		scala.collection.immutable.Set<String> immutableTopics = mutableTopics.toSet();
		scala.collection.immutable.Set<TopicAndPartition> scalaTopicAndPartitionSet = kafkaCluster
				.getPartitions(immutableTopics).right().get();
		scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = null;
		// 首次消费，默认取当前值,否则取上次保存值
		if (kafkaCluster.getConsumerOffsets(kafkaParams.get("group.id"), scalaTopicAndPartitionSet).isLeft()) {
			scala.collection.immutable.Map<TopicAndPartition, LeaderOffset> latestLeaderOffsetsmap = kafkaCluster
					.getLatestLeaderOffsets(scalaTopicAndPartitionSet).right().get();
			scala.collection.Iterator<Tuple2<TopicAndPartition, LeaderOffset>> it = latestLeaderOffsetsmap.iterator();
			Map<TopicAndPartition, Object> latestLeaderOffsetsmapJava = new HashMap<TopicAndPartition, Object>();
			while (it.hasNext()) {
				Tuple2<TopicAndPartition, LeaderOffset> tuple = it.next();
				latestLeaderOffsetsmapJava.put(tuple._1, tuple._2.offset());
			}
			consumerOffsetsTemp = scala.collection.JavaConverters
					.mapAsScalaMapConverter(latestLeaderOffsetsmapJava).asScala()
					.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
						private static final long serialVersionUID = 1L;

						public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
							return v1;
						}
					});
		} else {
			consumerOffsetsTemp = kafkaCluster
					.getConsumerOffsets(kafkaParams.get("group.id"), scalaTopicAndPartitionSet).right().get();
		}
		Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);
		Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions.setAsJavaSet(scalaTopicAndPartitionSet);
		for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet) {
			Long offset = (Long) consumerOffsets.get(topicAndPartition);
			fromOffsets.put(topicAndPartition, offset);
		}
		
		
		
		JavaDStream<byte[]> lines = KafkaUtils.createDirectStream(jssc, String.class, byte[].class, StringDecoder.class,
				DefaultDecoder.class, byte[].class, kafkaParams, fromOffsets,
				new Function<MessageAndMetadata<String, byte[]>, byte[]>() {

					private static final long serialVersionUID = 1L;

					public byte[] call(MessageAndMetadata<String, byte[]> v1) throws Exception {
						return v1.message();
					}
				});

		lines.foreachRDD(new VoidFunction<JavaRDD<byte[]>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<byte[]> Rdd) throws Exception {
				if (!Rdd.isEmpty()) {
					JavaRDD<Model_Base> modelRdd = Rdd.map(new Function<byte[], Model_Base>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Model_Base call(byte[] arg0) throws Exception {
							// TODO Auto-generated method stub
							return Clean.process(new String(arg0));
						}
					}).filter(new Function<Model_Base, Boolean>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(Model_Base arg0) throws Exception {
							// TODO Auto-generated method stub
							return arg0 != null;
						}
					}).persist(StorageLevel.DISK_ONLY());
					JavaRDD<String> classRdd = modelRdd.map(new Function<Model_Base, String>() {

						private static final long serialVersionUID = 1L;

						@Override
						public String call(Model_Base arg0) throws Exception {
							// TODO Auto-generated method stub
							return arg0.getClass().getSimpleName();
						}
					}).persist(StorageLevel.DISK_ONLY());
					Map<String, Integer> countMap = classRdd.aggregate(new HashMap<String, Integer>(),
							new Function2<Map<String, Integer>, String, Map<String, Integer>>() {

								private static final long serialVersionUID = 1L;

								@Override
								public Map<String, Integer> call(Map<String, Integer> map, String str)
										throws Exception {
									// TODO Auto-generated method stub
									if (map.containsKey(str)) {
										map.put(str, 1 + map.get(str));
									} else {
										map.put(str, 1);
									}
									return map;
								}
							}, new Function2<Map<String, Integer>, Map<String, Integer>, Map<String, Integer>>() {

								private static final long serialVersionUID = 1L;

								@Override
								public Map<String, Integer> call(Map<String, Integer> map0, Map<String, Integer> map1)
										throws Exception {
									// TODO Auto-generated method stub
									Iterator<Entry<String, Integer>> it = map1.entrySet().iterator();
									while (it.hasNext()) {
										Entry<String, Integer> entry = it.next();
										if (map0.containsKey(entry.getKey())) {
											map0.put(entry.getKey(), entry.getValue() + map0.get(entry.getKey()));
										} else {
											map0.put(entry.getKey(), entry.getValue());
										}
									}
									return map0;
								}
							});
					Iterator<Entry<String, Integer>> it = countMap.entrySet().iterator();
					while (it.hasNext()) {
						Entry<String, Integer> entry = it.next();
						HttpRequest.sendGet("http://192.168.1.55:8989/api/bigdata/setstatus/",
								"dataName=" + entry.getKey() + "&count=" + entry.getValue());
					}
					ETLStatics(countMap);

					classRdd.unpersist();

					JavaPairRDD<String, Model_Base> pairRdd = modelRdd
							.flatMap(new FlatMapFunction<Model_Base, Model_Base>() {

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
							}).mapToPair(new PairFunction<Model_Base, String, Model_Base>() {

								private static final long serialVersionUID = 1L;

								@Override
								public Tuple2<String, Model_Base> call(Model_Base mb) throws Exception {
									return new Tuple2<String, Model_Base>(mb.getClass().getSimpleName(), mb);
								}

							}).persist(StorageLevel.DISK_ONLY());
					// 获取进来的class的simplename
					List<String> classlist = pairRdd.keys().distinct().collect();
					// 获取配置中需要的class
					List<String> list = ToolUtils.getNeededClassSimpleName();
					if (list != null) {
						// 获取所有Model_Base的类
						Map<String, Class<?>> map = ClassUtils.getClassesExtends(beanPackage, Model_Base.class);
						for (String clasName : list) {
							// 对已配置的，本轮中有的数据存储入库
							if (classlist.contains(clasName)) {
								Class<?> cls = map.get(clasName);
								if (cls != null) {
									ETLWorker.run(pairRdd, cls);
								} else {
									logger.error("###########################找不到clasName类");
								}
							}
						}
					} else {
						logger.error("无法获取配置文件中需要的class");
					}
					pairRdd.unpersist();
					modelRdd.unpersist();

					OffsetRange[] offsets = ((HasOffsetRanges) Rdd.rdd()).offsetRanges();
					for (OffsetRange o : offsets) {
						// 封装topic.partition 与 offset对应关系 java Map
						TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
						Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
						topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());

						// 转换java map to scala immutable.map
						scala.collection.mutable.Map<TopicAndPartition, Object> map = JavaConversions
								.mapAsScalaMap(topicAndPartitionObjectMap);
						scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap = map
								.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
									private static final long serialVersionUID = 1L;

									public Tuple2<TopicAndPartition, Object> apply(
											Tuple2<TopicAndPartition, Object> v1) {
										return v1;
									}
								});

						// 更新offset到kafkaCluster
						kafkaCluster.setConsumerOffsets(group_id, scalatopicAndPartitionObjectMap);
					}
				}
			}
		});
		return jssc;
	}

	private static void ETLStatics(Map<String, Integer> classCountMap) {
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
