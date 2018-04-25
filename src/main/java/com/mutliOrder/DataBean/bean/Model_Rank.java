package com.mutliOrder.DataBean.bean;

import java.util.ArrayList;
import java.util.List;

import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.DataBean.bean.son.Model_RankCell;
import com.mutliOrder.DataBean.Constants;
import com.mutliOrder.DataBean.util.BeanUtils;

public class Model_Rank extends Model_Base {
	final static String Hbase_Table = "App_KeywordRanking";
	final static String Main_Rank_Hbase_Table = "App_MainRanking";
	final static String[] Unique_Key_Fields= new String[]{"RankType","TypeValue","batch"};
	private int RankType;
	private String TypeValue;
	private List<Model_RankCell> AppIdRank;

	public int getRankType() {
		return RankType;
	}

	public void setRankType(int rankType) {
		RankType = rankType;
	}

	public String getTypeValue() {
		return TypeValue;
	}

	public void setTypeValue(String typeValue) {
		TypeValue = typeValue;
	}

	public List<Model_RankCell> getAppIdRank() {
		return AppIdRank;
	}

	public void setAppIdRank(List<Model_RankCell> appIds) {
		AppIdRank = appIds;
	}
	
	@Override
	public List<Model_Base> split() {
		// TODO Auto-generated method stub
		List<Model_Base> list = new ArrayList<Model_Base>(1);
		//TODO
		return list;
	}

	public Model_Rank() {
		super();
		this.solrCollection=Hbase_Table;
		this.hbaseTable=Hbase_Table;
		this.uniqueKeyField=Unique_Key_Fields;
	}
	@Override
	public void clean() {
		batch = BeanUtils.getBatchNum(this.getFetchTime(), Constants.AN_HOURS_MILLIS);
	}
}
