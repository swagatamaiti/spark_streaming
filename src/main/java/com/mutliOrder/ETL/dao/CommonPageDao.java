package com.mutliOrder.ETL.dao;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;

import com.mutliOrder.DataBean.bean.base.Model_Base;

public interface CommonPageDao {
	void getRowFormString(final Model_Base mp, String collection, String query, String tableName, String family) throws Exception;
	void getRowsFormIndex(List<Model_Base> mpList, String ClassName, String collection, String query) throws Exception;
	boolean insert(Model_Base mp, String tableName, String family) throws  Exception;
	Model_Base getRowForUuid(Class<?> cls,String tableName, String uuid, String family,String qualifier) throws Exception;
	String getidForString(String collection, String query) throws IOException, SolrServerException;
	List<String> getonerowFromString(String collection,String query,String field) throws IOException, SolrServerException;
	boolean isExistForString(String collection, String query) throws IOException, SolrServerException;
	void getRowsFormString(List<Model_Base> mpList, String ClassName, String collection, String query, String tableName, String family) throws Exception;
	String getOneRow(String tableName, String rowkey, String family, String qualifier) throws Exception;
	boolean insert(String tableName, String rowKey, String family, String quailifer, String value) throws IOException;
	void deleteByKeys(String tableName, Object[] rows) throws Exception;
	void insert(List<Model_Base> mpList, String tableName, String family) throws  Exception;
	void insertModel_Base(Model_Base mp, String tableName, String family) throws  Exception;
}




