package com.mutliOrder.ETL.dao.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import com.google.gson.JsonSyntaxException;
import com.mutliOrder.DataBean.Clean;
import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.ETL.constant.Constants;
import com.mutliOrder.ETL.dao.CommonPageDao;
import com.mutliOrder.ETL.helper.HbaseHelper;
import com.mutliOrder.ETL.helper.HbaseHelper.QueryCallback;
import com.mutliOrder.ETL.helper.SolrHelper;
import com.mutliOrder.ETL.util.GenUuid;
import com.mutliOrder.ETL.util.ToolUtils;
import com.mutliOrder.common.String.StringUtils;
import com.mutliOrder.common.conf.ConfProperties;
import com.mutliOrder.common.conf.ConfigFactory;

public class CommonPageDaoImpl implements CommonPageDao {

	private static final List<String> superFieldList = new ArrayList<String>();
	private static Logger logger;
	private static final List<String> needFieldList = new ArrayList<String>();
	static {
		ConfProperties configProp = null;
		try {
			configProp = ConfigFactory.getInstance().getConfigProperties(Constants.LOG4J_CONFIG_PATH);
			String needList = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.NEED_FIELDS);
			String[] strs = needList.split(com.mutliOrder.common.String.Constants.COMMA);
			for(String str:strs){
				needFieldList.add(str);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		PropertyConfigurator.configure(configProp);
		logger = Logger.getLogger(CommonPageDaoImpl.class);
		Field[] superFields = Model_Base.class.getDeclaredFields();
		for (Field superField : superFields)
			superFieldList.add(superField.getName());
	}

	@Override
	public void getRowFormString(final Model_Base mp, String collection, String query, String tableName, String family)
			throws Exception {

		HbaseHelper hbaseHelper = HbaseHelper.getInstance();
		SolrHelper solrHelper = SolrHelper.getInstance();
		final String fly = family;                      
		String uuid = null;

		QueryResponse queryRes = solrHelper.getDoc(collection, query, "id", true,null,null);
		if (null != queryRes && 0 != queryRes.getResults().size()) {
			SolrDocument dc = queryRes.getResults().get(0);
			uuid = (String) dc.get("id");
		}
		final String rowKey = uuid;

		// logger.error("getRowFormString query = " + query +" rowkey = " +
		// rowKey + " class name = " + mp.getClass().getName());

		if (null == rowKey)
			return;

		Field[] comFields = mp.getClass().getDeclaredFields();
		final List<String> comFieldList = new ArrayList<>();
		for (Field filed : comFields)
			comFieldList.add(filed.getName());

		hbaseHelper.getOneRow(tableName, rowKey, new QueryCallback() {

			@Override
			public void process(List<Result> resultList) throws Exception {
				if (!resultList.isEmpty()) {
					Result result = resultList.get(0);
					Cell[] cells = result.rawCells();
					if (null != cells && cells.length > 0) {
						mp.setUuid(rowKey);
						for (Cell cell : cells) {
							String property = Bytes.toString(CellUtil.cloneQualifier(cell));
							String value = Bytes.toString(CellUtil.cloneValue(cell));
							String fm = Bytes.toString(CellUtil.cloneFamily(cell));
							if (fm.equals(fly)) {
								if (!StringUtils.filedIsNull(value)) {
									Field field = null;
									if (superFieldList.contains(property)) {
										field = mp.getClass().getSuperclass().getDeclaredField(property);
									} else if (comFieldList.contains(property)) {
										field = mp.getClass().getDeclaredField(property);
									}
									if (null != field && (field.getType().getSimpleName().equalsIgnoreCase("String"))) {
										field.setAccessible(true);
										field.set(mp, value);
									}
								}
							}
						}
					}
				}
			}
		});
	}

	@Override
	public boolean insert(Model_Base mp, String tableName, String family) throws Exception {
		if (null == mp.getUuid()) {
			mp.setUuid(GenUuid.GetUuid());
		}

		HbaseHelper hbaseHelper = HbaseHelper.getInstance();

		HashMap<String, String> qualMap = new HashMap<String, String>();
		String rowKey = mp.getUuid();
		boolean res = false;
		try {
			try {

				Field[] valueSelect = mp.getClass().getDeclaredFields();
				Field[] subSelect = Model_Base.class.getDeclaredFields();
				for (Field field : valueSelect) {
					field.setAccessible(true);
					String property = field.getName();
					;
					try {
						String value = null;
						if (field.get(mp) != null) {
							value = ToolUtils.getHbaseStringFromField(field.get(mp));
							if (!StringUtils.filedIsNull(value)) {
								qualMap.put(property, value);
							}
						}
					} catch (IllegalArgumentException | IllegalAccessException e) {
						logger.error(e);
					}
				}
				for (Field field : subSelect) {
					field.setAccessible(true);
					String property = field.getName();
					if (!(property.equals("delShouldSplitFlag") || property.equals("batchTime")
							|| property.equals("uuid"))) {
						try {
							String value = null;
							if (field.get(mp) != null) {
								value = ToolUtils.getHbaseStringFromField(field.get(mp));
								if (!StringUtils.filedIsNull(value)) {
									qualMap.put(property, value);
								}
							}
						} catch (IllegalArgumentException | IllegalAccessException e) {
							logger.error(e);
						}
					}
				}

			} catch (IllegalArgumentException e1) {
				logger.error(e1);
			}

		} catch (SecurityException e1) {
			logger.error(e1);
		}

		if (null != rowKey && qualMap.size() > 0) {
//		logger.error("tableName:" + tableName + " rowKey:" + rowKey + " family:" + family + " qualMap:" + qualMap);
			hbaseHelper.insert(tableName, rowKey, family, qualMap);
			res = true;
		}
		return res;
	}

	@Override
	public boolean insert(String tableName, String rowKey, String family, String quailifer, String value)
			throws IOException {

		HbaseHelper hbaseHelper = HbaseHelper.getInstance();
		boolean res = false;
		if (!(StringUtils.filedIsNull(tableName) || StringUtils.filedIsNull(rowKey) || StringUtils.filedIsNull(family)
				|| StringUtils.filedIsNull(quailifer) || StringUtils.filedIsNull(value))) {
			hbaseHelper.insert(tableName, rowKey, family, quailifer, value);
			res = true;
		}
		return res;
	}

	@Override
	public Model_Base getRowForUuid(Class<?> cls, String tableName, String uuid, String family,String qualifier) throws Exception {
		Model_Base mb = null;
		if(cls.getSuperclass().equals(Model_Base.class)){
			String rowKey = uuid;
			HbaseHelper hbaseHelper = HbaseHelper.getInstance();
			Result res = hbaseHelper.getOneRow(tableName, rowKey);
			if(res!=null){
				byte[] bytes = res.getValue(family.getBytes(), qualifier.getBytes());
				if(bytes!=null){
					try{
						mb = (Model_Base) com.mutliOrder.DataBean.Constants.gson.fromJson(Bytes.toString(bytes),cls);
						mb.setUuid(uuid);
					}catch(JsonSyntaxException e){
						logger.error(e);
					}
				}			
			}
		}
		return mb;
	}

	@Override
	public void getRowsFormIndex(List<Model_Base> mpList, String ClassName, String collection, String query)
			throws Exception {

		SolrHelper solrHelper = SolrHelper.getInstance();

		QueryResponse queryRes = solrHelper.getDoc(collection, query, null, false,null,null);
		if (null == queryRes) {
			return;
		}

		SolrDocumentList dcList = queryRes.getResults();

		for (SolrDocument dc : dcList) {
			Model_Base mp = (Model_Base) Class.forName(ClassName).newInstance();
			Field[] valueSelect = mp.getClass().getDeclaredFields();

			for (Field field : valueSelect) {
				field.setAccessible(true);
				String property = field.getName();
				if (dc.containsKey(property)) {
					String value = String.valueOf(dc.get(property));
					if (!StringUtils.filedIsNull(value))
						field.set(mp, value);
				}
			}

			mpList.add(mp);
		}
	}

	@Override
	public String getidForString(String collection, String query) throws IOException, SolrServerException {

		SolrHelper solrHelper = SolrHelper.getInstance();
		QueryResponse queryRes = null;
		//针对APP下架时，json中没有ReleaseDate字段，则获取最新的APP版本数据
		if("App_Base".equals(collection)&&!query.contains("ReleaseDate")){
			queryRes = solrHelper.getDoc(collection, query, "id", true,"batch",ORDER.desc);
		}else{
			queryRes = solrHelper.getDoc(collection, query, "id", true,null,null);
		}
		String rowKey = null;

		if (null != queryRes && 0 != queryRes.getResults().size()) {
			SolrDocument dc = queryRes.getResults().get(0);
			rowKey = (String) dc.get("id");
//			logger.error("rowKey from " + "collection = " + collection + "solr = " + rowKey + " query = " + query);
		}
		return rowKey;
	}

	@Override
	public boolean isExistForString(String collection, String query) throws IOException, SolrServerException {
		SolrHelper solrHelper = SolrHelper.getInstance();

		QueryResponse queryRes = solrHelper.getDoc(collection, query, "id", true,null,null);
		String rowKey = null;

		if (null != queryRes && 0 != queryRes.getResults().size()) {
			SolrDocument dc = queryRes.getResults().get(0);
			rowKey = (String) dc.get("id");
		}
		return (null != rowKey);
	}

	@Override
	public void getRowsFormString(List<Model_Base> mpList, String ClassName, String collection, String query,
			String tableName, String family) throws Exception {

		HbaseHelper hbaseHelper = HbaseHelper.getInstance();
		SolrHelper solrHelper = SolrHelper.getInstance();

		final String fly = family;
		List<String> listUuid = new ArrayList<>();
		QueryResponse qs = solrHelper.getDoc(collection, query, "id", false,null,null);
		if (qs != null) {
			SolrDocumentList queryRes = qs.getResults();
			if (null != queryRes) {
				for (SolrDocument dc : queryRes) {
					listUuid.add((String) dc.get("id"));
				}
			}
		}
		for (final String rowKey : listUuid) {

			final Model_Base mp = (Model_Base) Class.forName(ClassName).newInstance();
			Field[] comFields = mp.getClass().getDeclaredFields();
			final List<String> comFieldList = new ArrayList<>();
			for (Field filed : comFields)
				comFieldList.add(filed.getName());

			// logger.error("getRowFormString query = " + query +" rowkey = " +
			// rowKey + " class name = " + mp.getClass().getName());

			hbaseHelper.getOneRow(tableName, rowKey, new QueryCallback() {

				@Override
				public void process(List<Result> resultList) throws Exception {
					if (!resultList.isEmpty()) {
						Result result = resultList.get(0);
						Cell[] cells = result.rawCells();
						if (null != cells && cells.length > 0) {
							mp.setUuid(rowKey);
							for (Cell cell : cells) {
								String property = Bytes.toString(CellUtil.cloneQualifier(cell));
								String value = Bytes.toString(CellUtil.cloneValue(cell));
								String fm = Bytes.toString(CellUtil.cloneFamily(cell));
								try {
									if (fm.equals(fly)) {
										if (!StringUtils.filedIsNull(value)) {
											Field field = null;
											if (superFieldList.contains(property)) {
												field = mp.getClass().getSuperclass().getDeclaredField(property);
											} else if (comFieldList.contains(property)) {
												field = mp.getClass().getDeclaredField(property);
											}
											if (null != field
													&& (field.getType().getSimpleName().equalsIgnoreCase("String"))) {
												field.setAccessible(true);
												field.set(mp, value);
											}
										}
									}
								} catch (Exception e) {
									logger.error(e);
									throw new Exception();
								}
							}
						}
					}
				}
			});
			mpList.add(mp);
		}
	}

	@Override
	public String getOneRow(String tableName, String rowkey, String family, String qualifier) throws Exception {
		HbaseHelper hbaseHelper = HbaseHelper.getInstance();
		return hbaseHelper.getOneRow(tableName, rowkey, family, qualifier);
	}
	
	@Override
	public void deleteByKeys(String tableName, Object[] rows) throws Exception {
		HbaseHelper hbaseHelper = HbaseHelper.getInstance();
		hbaseHelper.deleteRows(tableName, rows);
	}
	
	
	@Override
	public void insert(List<Model_Base> mpList, String tableName, String family) throws Exception {
		Map<String,Map<String, String>> resMap = new HashMap<String,Map<String, String>>();
		for(Model_Base mp:mpList){
			if (null == mp.getUuid()) {
				mp.setUuid(GenUuid.GetUuid());
			}

			HbaseHelper hbaseHelper = HbaseHelper.getInstance();

			HashMap<String, String> qualMap = new HashMap<String, String>();
			String rowKey = mp.getUuid();
			try {
				try {

					Field[] valueSelect = mp.getClass().getDeclaredFields();
					Field[] subSelect = Model_Base.class.getDeclaredFields();
					for (Field field : valueSelect) {
						field.setAccessible(true);
						String property = field.getName();
						try {
							String value = null;
							if (field.get(mp) != null) {
								value = ToolUtils.getHbaseStringFromField(field.get(mp));
								if (!StringUtils.filedIsNull(value)) {
									qualMap.put(property, value);
								}
							}
						} catch (IllegalArgumentException | IllegalAccessException e) {
							logger.error(e);
						}
					}
					for (Field field : subSelect) {
						field.setAccessible(true);
						String property = field.getName();
						if (!(property.equals("delShouldSplitFlag") || property.equals("batchTime")
								|| property.equals("uuid"))) {
							try {
								String value = null;
								if (field.get(mp) != null) {
									value = ToolUtils.getHbaseStringFromField(field.get(mp));
									if (!StringUtils.filedIsNull(value)) {
										qualMap.put(property, value);
									}
								}
							} catch (IllegalArgumentException | IllegalAccessException e) {
								logger.error(e);
							}
						}
					}

				} catch (IllegalArgumentException e1) {
					logger.error(e1);
				}

			} catch (SecurityException e1) {
				logger.error(e1);
			}

			if (null != rowKey && qualMap.size() > 0) {
//			logger.error("tableName:" + tableName + " rowKey:" + rowKey + " family:" + family + " qualMap:" + qualMap);
				resMap.put(rowKey, qualMap);
			}
			hbaseHelper.insert(tableName, family,resMap);
		}
	}
	
	@Override
	public void insertModel_Base(Model_Base mp, String tableName, String family) throws Exception {
		Map<String,Map<String, String>> resMap = new HashMap<String,Map<String, String>>();

		if (null == mp.getUuid()) {
			mp.setUuid(GenUuid.GetUuid());
		}

		HbaseHelper hbaseHelper = HbaseHelper.getInstance();

		HashMap<String, String> qualMap = new HashMap<String, String>();
		String rowKey = mp.getUuid();
		try {
			try {
				String[] uniqueKeys = mp.getUniqueKeyField();
				Field[] subSelect = Model_Base.class.getDeclaredFields();
				String value = null;
				for(String property:uniqueKeys){
					try{
						Field field = mp.getClass().getDeclaredField(property);
						field.setAccessible(true);
						value = ToolUtils.getHbaseStringFromField(field.get(mp));
						if (!StringUtils.filedIsNull(value)) {
							qualMap.put(property, value);
						}			
					}catch(NoSuchFieldException|SecurityException e ){
						if(!"batch".equals(property)){
							logger.error(e);
						}
					}
						
				}
				for (Field field : subSelect) {
					field.setAccessible(true);
					String property = field.getName();
					if (needFieldList.contains(property)) {
						try {
							if (field.get(mp) != null) {
								value = ToolUtils.getHbaseStringFromField(field.get(mp));
								if (!StringUtils.filedIsNull(value)) {
									qualMap.put(property, value);
								}
							}
						} catch (IllegalArgumentException | IllegalAccessException e) {
							logger.error(e);
						}
					}
				}

			} catch (IllegalArgumentException e1) {
				logger.error(e1);
			}

		} catch (SecurityException e1) {
			logger.error(e1);
		}

		if (null != rowKey && qualMap.size() > 0) {
//		logger.error("tableName:" + tableName + " rowKey:" + rowKey + " family:" + family + " qualMap:" + qualMap);
			resMap.put(rowKey, qualMap);
		}
		hbaseHelper.insert(tableName, family,resMap);
	
	}
	
	public static void main(String[] args) throws Exception {
		CommonPageDaoImpl dao = new CommonPageDaoImpl();
		String json = null;
		File file = new File("C:/Users/Administrator/Desktop/test.txt");
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		json = br.readLine();
		br.close();
		Model_Base  mb = Clean.process(json);
		mb.setRefUuid(GenUuid.GetUuid());
		mb.setJson(com.mutliOrder.DataBean.Constants.gson.toJson(mb));
		dao.insertModel_Base(mb, "App_KeywordRanking", "cf");
	}

	@Override
	public List<String> getonerowFromString(String collection, String query,String field) throws IOException, SolrServerException {
		SolrHelper solrHelper = SolrHelper.getInstance();

		QueryResponse queryRes = solrHelper.getDoc(collection, query, null, true,null,null);
		
        List<String> result = new ArrayList<String>();
		if (null != queryRes && 0 != queryRes.getResults().size()) {

			SolrDocumentList dcList = queryRes.getResults();
			SolrDocument dc = dcList.get(0);
		    String value = String.valueOf(dc.get(field));
			String uuid = String.valueOf(dc.get("id"));

			result.add(value);
			result.add(uuid);
			
		}
		return result;
		}
	
	}

