package com.mutliOrder.ETL.util;

import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.google.gson.JsonSyntaxException;
import com.mutliOrder.DataBean.Constants;
import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.ETL.helper.RedisHelper;
import com.mutliOrder.common.String.StringUtils;
import com.mutliOrder.common.conf.ConfProperties;
import com.mutliOrder.common.conf.ConfigFactory;

import scala.Tuple2;
import scala.Tuple3;

public class ModelHbaseUtil {
	@SuppressWarnings("unused")
	private static Logger logger;
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
		logger = Logger.getLogger(ModelHbaseUtil.class);

	}

	public static JavaRDD<Tuple3<String, String, byte[]>> getNewJson(JavaHBaseContext hbaseContext, String tableName) {
		Scan scan = new Scan();
		scan.setCacheBlocks(false);
		scan.addColumn("cf".getBytes(), "json".getBytes());
		scan.addColumn("cf".getBytes(), "refUuid".getBytes());
		scan.addColumn("cf".getBytes(), "checkUuid".getBytes());
		JavaRDD<Tuple3<String, String, byte[]>> rdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan,
				new Function<Tuple2<ImmutableBytesWritable, Result>, Tuple3<String, String, byte[]>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<String, String, byte[]> call(Tuple2<ImmutableBytesWritable, Result> tuple)
							throws Exception {
						// TODO Auto-generated method stub
						String refuuid = Bytes.toString(tuple._2.getValue("cf".getBytes(), "refUuid".getBytes()));
						String checkUuid = Bytes.toString(tuple._2.getValue("cf".getBytes(), "checkUuid".getBytes()));
						if (refuuid != null && !refuuid.equals(checkUuid)) {
							return new Tuple3<String, String, byte[]>(Bytes.toString(tuple._1.copyBytes()), refuuid,
									tuple._2.getValue("cf".getBytes(), "json".getBytes()));
						}
						return null;
					}
				}).filter(new Function<Tuple3<String, String, byte[]>, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple3<String, String, byte[]> arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0 != null;
					}
				});
		return rdd;
	}

	public static JavaRDD<Model_Base> getCommonPageRddFromJson(JavaHBaseContext hbaseContext, final Class<?> cls,
			String tableName) {
		Scan scan = new Scan();
		scan.setCacheBlocks(false);
		scan.addColumn("cf".getBytes(), "json".getBytes());
		scan.addColumn("cf".getBytes(), "refUuid".getBytes());
		scan.addColumn("cf".getBytes(), "checkUuid".getBytes());
		JavaRDD<Model_Base> rdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan,
				new Function<Tuple2<ImmutableBytesWritable, Result>, Model_Base>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Model_Base call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
						Result res = tuple._2;
						String json = Bytes.toString(res.getValue("cf".getBytes(), "json".getBytes()));
						String refUuid = Bytes.toString(res.getValue("cf".getBytes(), "refUuid".getBytes()));
						String checkUuid = Bytes.toString(res.getValue("cf".getBytes(), "checkUuid".getBytes()));
						Model_Base mb = null;
						try {
							mb = (Model_Base) Constants.gson.fromJson(json, cls);
							mb.setRefUuid(refUuid);
							mb.setCheckUuid(checkUuid);
						} catch (JsonSyntaxException e) {
							e.printStackTrace();
						}
						return mb;
					}
				});
		return rdd;
	}

	public static JavaRDD<Model_Base> getCommonPageRddFromJson(JavaHBaseContext hbaseContext, final Class<?> cls,
			String tableName, Scan scan) {
		if (scan == null) {
			scan = new Scan();
		}
		scan.setCacheBlocks(false);
		scan.addColumn("cf".getBytes(), "json".getBytes());
		scan.addColumn("cf".getBytes(), "refUuid".getBytes());
		scan.addColumn("cf".getBytes(), "checkUuid".getBytes());
		JavaRDD<Model_Base> rdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan,
				new Function<Tuple2<ImmutableBytesWritable, Result>, Model_Base>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Model_Base call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
						Result res = tuple._2;
						String json = Bytes.toString(res.getValue("cf".getBytes(), "json".getBytes()));
						String refUuid = Bytes.toString(res.getValue("cf".getBytes(), "refUuid".getBytes()));
						String checkUuid = Bytes.toString(res.getValue("cf".getBytes(), "checkUuid".getBytes()));
						Model_Base mb = null;
						try {
							mb = (Model_Base) Constants.gson.fromJson(json, cls);
							mb.setRefUuid(refUuid);
							mb.setCheckUuid(checkUuid);
						} catch (JsonSyntaxException e) {
							e.printStackTrace();
						}
						return mb;
					}
				});
		return rdd;
	}

	public static JavaRDD<Model_Base> getCommonPageRdd(JavaHBaseContext hbaseContext, JavaSparkContext sc,
			final Class<?> cls, String tableName) throws SecurityException, ClassNotFoundException {

		Scan scan = new Scan();
		scan.setCacheBlocks(false);
		scan.addFamily("cf".getBytes());
		JavaRDD<Tuple2<ImmutableBytesWritable, Result>> javaRdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName),
				scan);

		Field[] subFields = cls.getDeclaredFields();
		List<String> subFieldList = new ArrayList<>();
		for (Field filed : subFields)
			subFieldList.add(filed.getName());

		Field[] superFields = Model_Base.class.getDeclaredFields();
		List<String> superFieldList = new ArrayList<>();
		for (Field superField : superFields)
			superFieldList.add(superField.getName());

		final Broadcast<List<String>> subFieldListBroadcast = sc.broadcast(subFieldList);
		final Broadcast<List<String>> superFieldListBroadcast = sc.broadcast(superFieldList);

		JavaRDD<Model_Base> commonRdd = javaRdd.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Model_Base>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Model_Base call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {

				List<String> subFieldList = subFieldListBroadcast.value();
				List<String> superFieldList = superFieldListBroadcast.value();

				Model_Base commonPage = (Model_Base) cls.newInstance();

				commonPage.setUuid(Bytes.toString(tuple._1.copyBytes()));
				Result result = tuple._2;
				Cell[] cells = result.rawCells();

				for (Cell cell : cells) {
					String property = Bytes.toString(CellUtil.cloneQualifier(cell));
					String value = Bytes.toString(CellUtil.cloneValue(cell));

					if (!StringUtils.filedIsNull(value)) {
						Field field = null;
						if (superFieldList.contains(property)) {
							field = commonPage.getClass().getSuperclass().getDeclaredField(property);
						} else if (subFieldList.contains(property)) {
							field = commonPage.getClass().getDeclaredField(property);
						}
						if (null != field && (field.getType().getSimpleName().equalsIgnoreCase("String")
								|| field.getType().getSimpleName().equalsIgnoreCase("Object"))) {
							field.setAccessible(true);
							field.set(commonPage, value);
						}
					}
				}
				return commonPage;
			}
		});

		return commonRdd;
	}
	/**
	 * 从redis 获取新增的uuid，再从hbase取出对应数据,，redis中uuid被hash后放在不同的key对应的set中
	 * 返回的tuple1为rowkey，tuple2为refuuid,tuple3为json
	 * @param hbaseContext
	 * @param redisKeyPattern 存放uuid的所有set对应的key的模式
	 * @param tableName
	 * @param sc
	 * @return
	 */
	public static JavaRDD<Tuple3<byte[], byte[], byte[]>> getNewJsonByHashRedis(JavaHBaseContext hbaseContext, String redisKeyPattern,
			String tableName,JavaSparkContext sc){
		JavaRDD<Tuple3<byte[], byte[], byte[]>> resRdd = sc.emptyRDD();
		try {
			RedisHelper redis = RedisHelper.getInstance(2);
			Set<String> keys = redis.keys(redisKeyPattern);
			 JavaRDD<String> rdd = sc.parallelize(new ArrayList<String>(keys));
			 JavaRDD<String> uuidRdd = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Iterable<String> call(Iterator<String> it) throws Exception {
					RedisHelper redis = RedisHelper.getInstance(2);
					Set<String> allSet = new HashSet<String>();
					while(it.hasNext()) {
						String  key = it.next();
						if("set".equals(redis.type(key))){
							allSet.addAll(redis.smembers(key));
						}
					}
					return new ArrayList<String>(allSet);
				}
			});
			resRdd = hbaseContext.bulkGet(TableName.valueOf(tableName), 100, uuidRdd, new Function<String, Get>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Get call(String str) throws Exception {
						Get get = new Get(str.getBytes());
						get.addColumn("cf".getBytes(), "json".getBytes());
						get.addColumn("cf".getBytes(), "refUuid".getBytes());
						return get;
					}
				}, new Function<Result, Tuple3<byte[], byte[], byte[]>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<byte[], byte[], byte[]> call(Result result) throws Exception {
						return new Tuple3<byte[], byte[], byte[]>(result.getRow(), result.getValue("cf".getBytes(), "refUuid".getBytes()), result.getValue("cf".getBytes(), "json".getBytes()));
					}
				});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resRdd;
	}
	
	/**
	 * 从redis 获取新增的uuid，再从hbase取出对应数据，uuid都放在同一个set中
	 * 返回的tuple1为rowkey，tuple2为refuuid,tuple3为json
	 * @param hbaseContext
	 * @param redisKey
	 * @param tableName
	 * @param sc
	 * @return
	 */
	public static JavaRDD<Tuple3<byte[], byte[], byte[]>> getNewJsonByRedis(JavaHBaseContext hbaseContext, String redisKey,
			String tableName,JavaSparkContext sc) {
		List<String> uuidList = new ArrayList<String>();
		JavaRDD<Tuple3<byte[], byte[], byte[]>>  rdd = sc.emptyRDD();
		try {
			RedisHelper redis = RedisHelper.getInstance(2); 
			Set<String> set = redis.smembers(redisKey);
			if(set!=null&&!set.isEmpty()) {
				set.remove(null);
				uuidList.addAll(set);
				rdd = getRddByUuid(hbaseContext, uuidList, tableName, sc);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rdd;
	}
	
	private static JavaRDD<Tuple3<byte[], byte[], byte[]>> getRddByUuid(JavaHBaseContext hbaseContext,List<String> uuidList,String tableName,JavaSparkContext sc) {
		JavaRDD<Tuple3<byte[], byte[], byte[]>>  rdd = sc.emptyRDD();
		if(uuidList!=null&&!uuidList.isEmpty()) {
			 JavaRDD<String> uuidRdd = sc.parallelize(uuidList);
			 rdd = hbaseContext.bulkGet(TableName.valueOf(tableName), 100, uuidRdd, new Function<String, Get>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Get call(String str) throws Exception {
					Get get = new Get(str.getBytes());
					get.addColumn("cf".getBytes(), "json".getBytes());
					get.addColumn("cf".getBytes(), "refUuid".getBytes());
					return get;
				}
			}, new Function<Result, Tuple3<byte[], byte[], byte[]>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple3<byte[], byte[], byte[]> call(Result result) throws Exception {
					return new Tuple3<byte[], byte[], byte[]>(result.getRow(), result.getValue("cf".getBytes(), "refUuid".getBytes()), result.getValue("cf".getBytes(), "json".getBytes()));
				}
			});
		}
		return rdd;
	}
}
