package com.mutliOrder.ETL.app.worker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.mutliOrder.DataBean.bean.Model_AppInfo;
import com.mutliOrder.DataBean.bean.Model_Rank;
import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.ETL.constant.Constants;
import com.mutliOrder.ETL.dao.DAOFactory;
import com.mutliOrder.ETL.helper.RedisHelper;
import com.mutliOrder.ETL.util.GenUuid;
import com.mutliOrder.ETL.util.ToolUtils;
import com.mutliOrder.common.String.StringUtils;
import com.mutliOrder.common.conf.ConfProperties;
import com.mutliOrder.common.conf.ConfigFactory;

import scala.Tuple2;

public class ETLWorker {

	private static Logger logger;
	private static String family = "cf";
	private static Map<String, Set<String>> fieldClassMap = null;
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
		logger = Logger.getLogger(ETLWorker.class);
		String beanPackage = null;
		try {
			beanPackage = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
					.getProperty(Constants.BEAN_PACKAGE);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		fieldClassMap = ToolUtils.getClassFields(beanPackage, Model_Base.class);
		try {
			family = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
					.getProperty(Constants.FAMILY);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * spark去重存储流程
	 * 
	 * @param pairRdd
	 * @param cls
	 */
	public static void run(JavaPairRDD<String, Model_Base> pairRdd, Class<?> cls) {

		// 过滤对应class的rdd
		pairRdd.filter(new Function<Tuple2<String, Model_Base>, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Model_Base> tuple) throws Exception {
				return cls.getSimpleName().equals(tuple._1);
			}
		}).mapToPair(new PairFunction<Tuple2<String, Model_Base>, String, Model_Base>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Model_Base> call(Tuple2<String, Model_Base> tuple) throws Exception {
				return new Tuple2<String, Model_Base>(getUniqueKey(tuple._2), tuple._2);
			}
			// 根据唯一字段去重
		}).reduceByKey(new Function2<Model_Base, Model_Base, Model_Base>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Model_Base call(Model_Base arg0, Model_Base arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0.merge(arg1);
			}
		}).mapToPair(new PairFunction<Tuple2<String, Model_Base>, String, Model_Base>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Model_Base> call(Tuple2<String, Model_Base> tuple) throws Exception {
				return new Tuple2<String, Model_Base>(getUuid(tuple._2), tuple._2);
			}

		}).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Model_Base>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Iterator<Tuple2<String, Model_Base>> it) throws Exception {
				// TODO Auto-generated method stub
				
				String hbase = null;
				int i = 0;
				//针对数据比较多,但每条数据量小的数据，散列化存入redis，后续分批次上传
				if(Model_Rank.class.equals(cls)) {
					Map<String,Set<String>> map = new HashMap<String,Set<String>>();
					String uuid = null;
					while (it.hasNext()) {
						Tuple2<String, Model_Base> tuple2 = it.next();
						hbase = tuple2._2.getHbaseTable();
						uuid = save(tuple2);
						if(uuid!=null) {
							hbase=hbase+uuid.substring(0, 2);
							if(map.containsKey(hbase)) {
								map.get(hbase).add(uuid);
							}else {
								Set<String> set = new HashSet<String>();
								set.add(uuid);
								map.put(hbase, set);
							}
						}
					}
					if(map!=null&&!map.isEmpty()) {
						RedisHelper.getInstance(2).saddPipeline(map);
					}
				}else {
					Set<String> set = new HashSet<String>();
					while (it.hasNext()) {
						Tuple2<String, Model_Base> tuple2 = it.next();
						if (i == 0) {
							hbase = tuple2._2.getHbaseTable();
							i++;
						}
						set.add(save(tuple2));
					}
					set.remove(null);
					if (set.size() != 0 && hbase != null) {
						RedisHelper.getInstance(2).sadd(hbase, set.toArray(new String[0]));
					}
				}
			}
		});
	};

	/**
	 * 从配置读取唯一性字段，组成key
	 * 
	 * @param mb
	 * @return
	 */
	private static String getUniqueKey(Model_Base mb) {
		String[] uniqueKeys = mb.getUniqueKeyField();
		String key = "";
		for (String fieldName : uniqueKeys) {
			Field field;
			Class<?> cls = null;
			if (fieldClassMap != null && fieldClassMap.get(mb.getClass().getSimpleName()).contains(fieldName)) {
				cls = mb.getClass();
			} else {
				cls = Model_Base.class;
			}
			try {
				field = cls.getDeclaredField(fieldName);
				field.setAccessible(true);
				if (field.get(mb) != null) {
					key += field.get(mb).toString();
				}

			} catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException e) {

				logger.error(e);
			}
		}
		return key;

	}

	/**
	 * 从solr根据唯一字段检查是否存在此条记录，存在返回记录uuid，不存在自动生成uuid
	 * 
	 * @param mb
	 * @return
	 * @throws SolrServerException
	 */
	private static String getUuid(Model_Base mb) throws SolrServerException {
		String query = getSolrQuery(mb.getUniqueKeyField(), mb);
		String uuid = null;
		try {
			uuid = DAOFactory.getCommonPageDao().getidForString(mb.getSolrCollection(), query.toString());
		} catch (IOException e) {
			logger.error(e);
		}
		// if(uuid==null){
		// uuid=GenUuid.GetUuid();
		// }
		return uuid;
	}

	/**
	 * 将hbase的数据和当前数据合并后存入数据库
	 * 
	 * @param tuple
	 */
	private static String save(Tuple2<String, Model_Base> tuple) {
		String redisId = null;
		try {
			// 不更新
			if (tuple._1 == null && !Model_AppInfo.class.equals(tuple._2.getClass())) {
				Model_Base mb = tuple._2;
				mb.setUuid(GenUuid.GetUuid());
				mb.setRefUuid(GenUuid.GetUuid());
				mb.setJson(com.mutliOrder.DataBean.Constants.gson.toJson(mb));
				DAOFactory.getCommonPageDao().insertModel_Base(mb, tuple._2.getHbaseTable(), family);
				redisId = mb.getUuid();
				// 需要更新
			} else if (Model_AppInfo.class.equals(tuple._2.getClass())) {
				// 获取数据库里的对应数据
				String uuid = null;
				if (tuple._1 == null) {
					uuid = GenUuid.GetUuid();
				} else {
					uuid = tuple._1;
				}
				Model_Base mp = DAOFactory.getCommonPageDao().getRowForUuid(tuple._2.getClass(),
						tuple._2.getHbaseTable(), uuid, family, "json");
				Model_Base mb = tuple._2;
				if (mp == null && mb != null) {
					mb.setUuid(uuid);
					mb.setRefUuid(GenUuid.GetUuid());
					mb.setJson(com.mutliOrder.DataBean.Constants.gson.toJson(mb));
					DAOFactory.getCommonPageDao().insertModel_Base(mb, tuple._2.getHbaseTable(), family);
					redisId = mb.getUuid();
				}else if (mp != null) {
					mb = mp.merge(mb);
					if (mb != null && !mb.equals(mp)) {
						mb.setUuid(uuid);
						mb.setRefUuid(GenUuid.GetUuid());
						mb.setJson(com.mutliOrder.DataBean.Constants.gson.toJson(mb));
						DAOFactory.getCommonPageDao().insertModel_Base(mb, tuple._2.getHbaseTable(), family);
						redisId = mb.getUuid();
					}
				}
			}

		} catch (Exception e) {
			logger.error(e);
		}
		return redisId;
	}

	/**
	 * 从mb中获取list包含的字段组成solrquery.
	 * 
	 * @param list
	 * @param mb
	 * @return
	 */
	private static String getSolrQuery(String[] list, Model_Base mb) {
		StringBuilder query = new StringBuilder();
		int i = 0;
		for (String fieldName : list) {
			Field field;
			Class<?> cls = null;
			if (fieldClassMap != null && fieldClassMap.get(mb.getClass().getSimpleName()).contains(fieldName)) {
				cls = mb.getClass();
			} else {
				cls = Model_Base.class;
			}
			try {
				field = cls.getDeclaredField(fieldName);
				field.setAccessible(true);
				String value = null;
				// 此处将对象里的数据转换成String存到hbase
				value = ToolUtils.getHbaseStringFromField(field.get(mb));
				if (i != 0 && !StringUtils.filedIsNull(value)) {
					query.append(" AND ");
				}
				if (!StringUtils.filedIsNull(value)) {
					query.append(
							fieldName + ":\"" + value.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"") + "\"");
					i++;
				}
			} catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException e) {
				logger.error(e);
			}

		}
		return query.toString();
	}
}
