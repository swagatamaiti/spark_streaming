package com.mutliOrder.DataBean.bean.base;

public class BaseBean {
	private String ContentType;
	private Object Content;
	private String FetchTaskId;
	private String FetchTime;
	private String AppleStoreFront;

	public String getAppleStoreFront() {
		return AppleStoreFront;
	}

	public void setAppleStoreFront(String appleStoreFront) {
		AppleStoreFront = appleStoreFront;
	}

	public String getContentType() {
		return ContentType;
	}

	public void setContentType(String contentType) {
		ContentType = contentType;
	}

	public Object getContent() {
		return Content;
	}

	public void setContent(Object content) {
		Content = content;
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

}
