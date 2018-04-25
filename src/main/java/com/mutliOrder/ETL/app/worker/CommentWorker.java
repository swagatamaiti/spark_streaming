package com.mutliOrder.ETL.app.worker;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.DataBean.bean.son.Model_CommentContent;
import com.mutliOrder.ETL.constant.Constants;
import com.mutliOrder.ETL.dao.CommonPageDao;
import com.mutliOrder.ETL.dao.DAOFactory;
import com.mutliOrder.ETL.helper.HbaseHelper;
import com.mutliOrder.ETL.helper.RedisHelper;
import com.mutliOrder.ETL.util.GenUuid;
import com.mutliOrder.ETL.util.MD5Utils;
import com.mutliOrder.common.conf.ConfProperties;
import com.mutliOrder.common.conf.ConfigFactory;

import scala.Tuple2;

public class CommentWorker {
	@SuppressWarnings("unused")
	private static Logger logger;
	private static String family = "cf";
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
		logger = Logger.getLogger(CommentWorker.class);
		try {
			family = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
					.getProperty(Constants.FAMILY);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void run(JavaRDD<Model_Base> rdd) {
		rdd.mapToPair(new PairFunction<Model_Base, Long, Model_Base>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Model_Base> call(Model_Base mb) throws Exception {
				Model_CommentContent mcc = (Model_CommentContent) mb;
				return new Tuple2<Long, Model_Base>(Long.valueOf(mcc.getUserReviewId()), mcc);
			}
		}).reduceByKey(new Function2<Model_Base, Model_Base, Model_Base>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Model_Base call(Model_Base arg0, Model_Base arg1) throws Exception {
				return arg0.merge(arg1);
			}
		}).map(new Function<Tuple2<Long, Model_Base>, Model_Base>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Model_Base call(Tuple2<Long, Model_Base> arg0) throws Exception {
				return arg0._2;
			}
		}).filter(new Function<Model_Base, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Model_Base arg0) throws Exception {
				HbaseHelper hbase = HbaseHelper.getInstance();
				Model_CommentContent mcc = (Model_CommentContent) arg0;
				String uuid = MD5Utils.getMD5(mcc.getUserReviewId()).substring(0, 22) + mcc.getUserReviewId();
				mcc.setUuid(uuid);
				return !hbase.checkRowkeyExist(mcc.getHbaseTable(), uuid);
			}
		}).foreachPartition(new VoidFunction<Iterator<Model_Base>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Iterator<Model_Base> arg0) throws Exception {
				CommonPageDao dao = DAOFactory.getCommonPageDao();
				Map<String,Set<String>> map = new HashMap<String,Set<String>>();
				String hbase = null;
				int i = 0;
				String key = null;
				while (arg0.hasNext()) {
					Model_CommentContent mcc = (Model_CommentContent) arg0.next();
					if(i==0) {
						hbase = mcc.getHbaseTable();
						i++;
					}
					if (mcc.getUuid() == null) {
						mcc.setUuid(MD5Utils.getMD5(mcc.getUserReviewId()).substring(0, 22) + mcc.getUserReviewId());
					}
					key = hbase+mcc.getUuid().substring(0,2);
					if(map.containsKey(key)) {
						map.get(key).add(mcc.getUuid());
					}else {
						Set<String> set = new HashSet<String>();
						set.add(mcc.getUuid());
						map.put(key, set);
					}
					mcc.setRefUuid(GenUuid.GetUuid());
					mcc.setJson(mcc.toString());
					dao.insertModel_Base(mcc, hbase, family);
				}
				if(map!=null&&!map.isEmpty()) {
					RedisHelper.getInstance(2).saddPipeline(map);
				}
			}
		});
		
	}
}
