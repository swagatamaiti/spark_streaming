package com.mutliOrder.DataBean.bean.son;

import com.mutliOrder.DataBean.bean.base.Model_Base;

public class Model_CommentContent extends Model_Base{
	final static String Hbase_Table = "App_Comment";
	final static String[] Unique_Key_Fields= new String[]{"userReviewId"};
//	private String body;
//	private String customerType;
	private String date;
//	private boolean isEdited;
//	private String name;
	private String rating;
//	private Object reportConcernReasons;
//	private String title;
	private String userReviewId;
//	private String voteCount;
//	private String voteSum;
	private String appid;
	private int total;
//	public String getBody() {
//		return body;
//	}
//	public void setBody(String body) {
//		this.body = body;
//	}
//	public String getCustomerType() {
//		return customerType;
//	}
//	public void setCustomerType(String customerType) {
//		this.customerType = customerType;
//	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
//	public boolean isEdited() {
//		return isEdited;
//	}
//	public void setEdited(boolean isEdited) {
//		this.isEdited = isEdited;
//	}
//	public String getName() {
//		return name;
//	}
//	public void setName(String name) {
//		this.name = name;
//	}
	public String getRating() {
		return rating;
	}
	public void setRating(String rating) {
		this.rating = rating;
	}
//	public Object getReportConcernReasons() {
//		return reportConcernReasons;
//	}
//	public void setReportConcernReasons(Object reportConcernReasons) {
//		this.reportConcernReasons = reportConcernReasons;
//	}
//	public String getTitle() {
//		return title;
//	}
//	public void setTitle(String title) {
//		this.title = title;
//	}
	public String getUserReviewId() {
		return userReviewId;
	}
	public void setUserReviewId(String userReviewId) {
		this.userReviewId = userReviewId;
	}
//	public String getVoteCount() {
//		return voteCount;
//	}
//	public void setVoteCount(String voteCount) {
//		this.voteCount = voteCount;
//	}
//	public String getVoteSum() {
//		return voteSum;
//	}
//	public void setVoteSum(String voteSum) {
//		this.voteSum = voteSum;
//	}
	public String getAppid() {
		return appid;
	}
	public void setAppid(String appid) {
		this.appid = appid;
	}
	public int getTotal() {
		return total;
	}
	public void setTotal(int total) {
		this.total = total;
	}
	public Model_CommentContent() {
		super();
		this.hbaseTable = Hbase_Table;
		this.solrCollection=Hbase_Table;
		this.uniqueKeyField = Unique_Key_Fields;
	}
	@Override
	public Model_Base merge(Model_Base mb) {
		Model_CommentContent mcc = (Model_CommentContent)mb;
		Model_CommentContent thisMcc = (Model_CommentContent)this;
		if (mcc.getDate() != null && thisMcc.getDate() != null) {
			if (thisMcc.getDate().compareTo(mcc.getDate()) >= 0) {
				return this;
			} else {
				return mb;
			}
		} else if (thisMcc.getDate() == null) {
			return mb;
		} else {
			return this;
		}
	}
}
