package com.mutliOrder.DataBean.bean;

import java.util.ArrayList;
import java.util.List;

import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.DataBean.bean.son.Model_CommentContent;
import com.mutliOrder.common.Time.TimeUtils;

public class Model_CommentContentList extends Model_Base{
	final static String Hbase_Table = "App_Comment";
	final static String[] Unique_Key_Fields= new String[]{"AppId","batch"};
	private String AppId;
	private int Total;
	private List<Model_CommentContent> userReviewList;

	public int getTotal() {
		return Total;
	}

	public void setTotal(int total) {
		Total = total;
	}

	public String getAppId() {
		return AppId;
	}

	public void setAppId(String appId) {
		AppId = appId;
	}

	public List<Model_CommentContent> getUserReviewList() {
		return userReviewList;
	}

	public void setUserReviewList(List<Model_CommentContent> userReviewList) {
		this.userReviewList = userReviewList;
	}
	
	public Model_CommentContentList() {
		this.hbaseTable=Hbase_Table;
		this.solrCollection=Hbase_Table;
		this.uniqueKeyField = Unique_Key_Fields;
	}
	@Override
	public List<Model_Base> split() {
		// TODO Auto-generated method stub
		List<Model_Base> list = null;
		if(this.getUserReviewList()!=null){
			list = new ArrayList<Model_Base>(this.getUserReviewList().size());
			int  size = this.getUserReviewList().size();
			for(int i=0;i<size;i++){
				Model_CommentContent mcc = this.getUserReviewList().get(i);
				mcc.setAppid(this.AppId);
				mcc.setTotal(this.Total);
				if(!mcc.getDate().contains("-")){
					mcc.setDate(TimeUtils.formatUSDate(mcc.getDate()));
				}
				if(mcc.getDate()!=null&&mcc.getDate().length()>10){
					mcc.setBatch(mcc.getDate().substring(0, 10).replaceAll("-", ""));
				}
				list.add(mcc);
			}
		}
		return list;
	}
}
