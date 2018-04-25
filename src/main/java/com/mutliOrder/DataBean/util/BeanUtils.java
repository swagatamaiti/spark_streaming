package com.mutliOrder.DataBean.util;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import com.mutliOrder.DataBean.Constants;
import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.common.Class.ClassUtils;
import com.mutliOrder.common.Time.TimeUtils;

public class BeanUtils {
	private static Map<String, Class<?>> beanMap = null;
	static {
		try {
			beanMap = ClassUtils.getClassBySimpleName("com.multiOrder.DataBean.bean");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Class<?> getClassByType(String type) throws ClassNotFoundException {
		if (beanMap == null) {
			return null;
		}
		return beanMap.get(type);
	}

	public static String getBatchNum(String date, long batchTime) {
		if (date == null || batchTime <= 0) {
			return "";
		}
		Date d = TimeUtils.getDateFromYYYYMMDD_hhmmss(date);
		if (d != null&&batchTime==Constants.AN_HOURS_MILLIS) {
			return TimeUtils
					.getYYYYMMDDHHFromMillis(d.getTime());
		}else if(d != null&&batchTime==Constants.ONE_DAY_MILLIS){
			return TimeUtils
					.getYYYYMMDD00FromMillis(d.getTime());
		}
		return "";
	}

	public static void setBase(Model_Base son, Model_Base father) {
		son.setFetchTaskId(father.getFetchTaskId());
		son.setAppleStoreFront(father.getAppleStoreFront());
		son.setFetchTime(father.getFetchTime());
		son.setBatch(father.getBatch());
	}

	/**
	 * 在同类型下，比较本类字段（不包括父类的字段）和batch字段是否一致
	 * 
	 * @param mb1
	 * @param mb2
	 * @return
	 */
	public static boolean isModelBaseEqual(Model_Base mb1, Model_Base mb2) {
		if (mb1 != null && mb2 != null) {
			if (mb1.getClass().getName().equals(mb2.getClass().getName())) {
				try {
					Model_Base thisClone = (Model_Base) mb1.myClone();
					Model_Base mbClone = (Model_Base) mb2.myClone();
					String thisCloneJson = Constants.gson.toJson(getCompareModelBase(thisClone));
					String mbCloneJson = Constants.gson.toJson(getCompareModelBase(mbClone));
					if (thisCloneJson != null && thisCloneJson.equals(mbCloneJson)) {
						return true;
					}
				} catch (CloneNotSupportedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return false;
	}

	public static void main(String[] args) {
		System.out.println(getBatchNum("2017-05-15 04:00:00", 12 * 60 * 60 * 1000));
		System.out.println(4 / 3 * 3);
		;
	}

	public static Model_Base getCompareModelBase(Model_Base mb) {
		if (mb != null) {
			mb.setRefUuid(null);
			mb.setUuid(null);
			mb.setFetchTaskId(null);
			mb.setAppleStoreFront(null);
			mb.setCheckUuid(null);
			mb.setJson(null);
			return mb;
		}
		return null;
	}
}
