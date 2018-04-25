package com.mutliOrder.DataBean.bean.base;

import java.util.ArrayList;
import java.util.List;

import com.mutliOrder.DataBean.Constants;
import com.mutliOrder.DataBean.util.BeanUtils;

/**
 * bean基础类
 * 
 * @author Administrator
 *
 */
public class Model_Base implements Cloneable {

	public static transient long batchTime = Constants.AN_HOURS_MILLIS;

	protected String FetchTaskId;
	protected String FetchTime;
	protected String uuid;
	protected String batch;
	protected String AppleStoreFront;
	protected String esId;
	protected transient String refUuid;
	protected transient String checkUuid;
	protected transient String[] uniqueKeyField;
	protected transient String hbaseTable;
	protected transient String solrCollection;
	protected transient String json;


	public String getEsId() {
		return esId;
	}

	public void setEsId(String esId) {
		this.esId = esId;
	}

	public String getSolrCollection() {
		return solrCollection;
	}

	public void setSolrCollection(String solrCollection) {
		this.solrCollection = solrCollection;
	}

	public String getJson() {
		return json;
	}

	public void setJson(String json) {
		this.json = json;
	}

	public String getHbaseTable() {
		return hbaseTable;
	}

	public void setHbaseTable(String hbaseTable) {
		this.hbaseTable = hbaseTable;
	}

	public String getAppleStoreFront() {
		return AppleStoreFront;
	}

	public void setAppleStoreFront(String appleStoreFront) {
		AppleStoreFront = appleStoreFront;
	}

	public String getBatch() {
		return batch;
	}

	public void setBatch(String batch) {
		this.batch = batch;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getFetchTaskId() {
		return FetchTaskId;
	}

	public void setFetchTaskId(String fetchTaskId) {
		FetchTaskId = fetchTaskId;
	}

	public String getFetchTime() {
		return FetchTime;
	}

	public void setFetchTime(String fetchTime) {
		FetchTime = fetchTime;
	}

	public String getRefUuid() {
		return refUuid;
	}

	public void setRefUuid(String refUuid) {
		this.refUuid = refUuid;
	}

	public String getCheckUuid() {
		return checkUuid;
	}

	public void setCheckUuid(String checkUuid) {
		this.checkUuid = checkUuid;
	}

	public String[] getUniqueKeyField() {
		return uniqueKeyField;
	}

	public void setUniqueKeyField(String[] uniqueKeyField) {
		this.uniqueKeyField = uniqueKeyField;
	}

	/**
	 * 在内存去重后执行的分离操作，即分离后的数据不会经过内存去重
	 * 
	 * @return
	 */
	public List<Model_Base> split() {
		List<Model_Base> list = new ArrayList<Model_Base>(1);
		list.add(this);
		return list;
	}

	@Override
	public String toString() {
		return Constants.gson.toJson(this);
	}
	
	/**
	 * 把2个Model_Base合并，默认this是数据库里的，传参是新进来的，默认取fetchtime大的数据
	 * 
	 * @param mb
	 * @return
	 */
	public Model_Base merge(Model_Base mb) {

		if (mb.getFetchTime() != null && this.getFetchTime() != null) {
			if (this.getBatch().compareTo(mb.getBatch()) >= 0) {
				return this;
			} else {
				return mb;
			}
		} else if (this.getBatch() == null) {
			return mb;
		} else {
			return this;
		}
	}

	/**
	 * 清洗逻辑
	 */
	public void clean() {
		batch = BeanUtils.getBatchNum(this.getFetchTime(), batchTime);
	}

	public Model_Base() {
		super();
	}

	public static void main(String[] args) {
		System.out.println("3".compareTo("2"));
	}

	public Model_Base myClone() throws CloneNotSupportedException {
		return (Model_Base) this.clone();
	}

}
