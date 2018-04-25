package com.mutliOrder.ETL.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.Set;

import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.ETL.constant.Constants;
import com.mutliOrder.common.Class.ClassUtils;
import com.mutliOrder.common.String.StringUtils;
import com.mutliOrder.common.Time.TimeUtils;
import com.mutliOrder.common.conf.ConfProperties;
import com.mutliOrder.common.conf.ConfigFactory;
/**
 * 字符工具类
 *
 */
public class ToolUtils {
	private static String family = "cf";
	private static Logger logger;
	static{
		ConfProperties configProp = null;
		try {
			configProp = ConfigFactory.getInstance().getConfigProperties(Constants.LOG4J_CONFIG_PATH);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		PropertyConfigurator.configure(configProp);
		logger = Logger.getLogger(ToolUtils.class);
		try {
			family = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
					.getProperty(Constants.FAMILY);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 从配置读取list与append拼接后对应值，并按split分割形成map
	 * 
	 * @param list
	 * @param append
	 * @param split
	 * @return
	 */
	public static Map<String, Set<String>> getMapSetFromProperty(List<String> list, String append, String split) {
		Map<String, String> map = getMapFromProperty(list, append);
		Iterator<Entry<String, String>> it = map.entrySet().iterator();
		Map<String, Set<String>> resultMap = new HashMap<String, Set<String>>();
		while (it.hasNext()) {
			Entry<String, String> entry = it.next();
			if (!StringUtils.filedIsNull(entry.getValue())) {
				Set<String> set = new HashSet<String>();
				String[] fields = entry.getValue().split(split);
				for (String field : fields) {
					set.add(field);
				}
				resultMap.put(entry.getKey(), set);
			}
		}
		return resultMap;
	}
	
	/**
	 * 将list每个元素和append拼接后去配置文件中找到对应值
	 * 
	 * @param list
	 * @param append
	 * @return
	 */
	public static Map<String, String> getMapFromProperty(List<String> list, String append) {
		Map<String, String> map = new HashMap<String, String>();
		for (String str : list) {
			String property = null;
			try {
				property = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
						.getProperty(str + append);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (property != null) {
				map.put(str, property);
			}
		}
		return map;
	}

	/**
	 * 从配置读取class.simplename唯一字段, properties中配置名“bean的简单类名.uniqueFields”，
	 * 值填写唯一字段，与bean的字段对应
	 * 
	 * @param list
	 * @return
	 */
	public static Map<String, Set<String>> getUniqueKeyMap(List<String> list) {
		return getMapSetFromProperty(list, com.mutliOrder.common.String.Constants.DOT + Constants.UNIQUE_FIELDS,
				com.mutliOrder.common.String.Constants.COMMA);
	}

	/**
	 * 从配置读取class.simplename唯一字段, properties中配置名“bean的简单类名.delUniqueFields”，
	 * 值填写唯一字段，与bean的字段对应
	 * 
	 * @param list
	 * @return
	 */
	public static Map<String, Set<String>> getDelUniqueKeyMap(List<String> list) {
		return getMapSetFromProperty(list, com.mutliOrder.common.String.Constants.DOT + Constants.DEL_UNIQUE_FIELDS,
				com.mutliOrder.common.String.Constants.COMMA);
	}

	/**
	 * 获取bean对应的solr的collection properties中的key为“bean的简单类名.collection”
	 * 
	 * @param list
	 * @return
	 */
	public static Map<String, String> getCollection(List<String> list) {
		return getMapFromProperty(list, com.mutliOrder.common.String.Constants.DOT + Constants.COLLECTION);
	}

	/**
	 * 获取bean对应的solr的collection properties中的key为“bean的简单类名.table”
	 * 
	 * @param list
	 * @return
	 */
	public static Map<String, String> getTableName(List<String> list) {
		return getMapFromProperty(list, com.mutliOrder.common.String.Constants.DOT + Constants.TABLE);
	}

	/**
	 * 获取配置文件中需要的class
	 * 
	 * @return
	 */
	public static List<String> getNeededClassSimpleName() {
		List<String> list = null;
		String info = null;
		try {
			info = ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
					.getProperty(Constants.CLASS_INFO);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (info != null) {
			list = new ArrayList<String>();
			String[] strs = info.split(com.mutliOrder.common.String.Constants.COMMA);
			for (String str : strs) {
				list.add(str);
			}
		}
		return list;
	}

	/**
	 * 将数据格式化成hbase需要的String。date转成yyy_MM_dd HH:mm:ss。其他转成对应json
	 * 
	 * @param obj
	 * @param clasType
	 * @return
	 */
	public static String getHbaseStringFromField(Object obj) {
		String value = null;
		if (obj == null) {
			
		} else if (Date.class.equals(obj.getClass())) {
			value = TimeUtils.getYYYYMMDD_hhmmss((Date) obj);
		} else if (String.class.equals(obj.getClass())) {
			value = (String) obj;
		} else if(Integer.class.equals(obj.getClass())){
			value = String.valueOf(obj);
		} else if(Long.class.equals(obj.getClass())){
			value = String.valueOf(obj);
		} else if(Double.class.equals(obj.getClass())){
			value = String.valueOf(obj);
		} else if(Float.class.equals(obj.getClass())){
			value = String.valueOf(obj);
		} else if(Boolean.class.equals(obj.getClass())){
			value = String.valueOf(obj);
		}else {
			value = com.mutliOrder.DataBean.Constants.gson.toJson(obj);
		}
		return value;
	}

	/**
	 * 获取继承某类的某包下的所有class的的属性
	 * 
	 * @return
	 */
	public static Map<String, Set<String>> getClassFields(String packeName, Class<?> cls) {
		if (StringUtils.filedIsNull(packeName) || cls == null) {
			return null;
		}
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		try {
			Set<Entry<String, Class<?>>> classMapEntry = ClassUtils.getClassesExtends(packeName, cls).entrySet();
			for (Entry<String, Class<?>> entry : classMapEntry) {
				Set<String> set = new HashSet<String>();
				Field[] fields = entry.getValue().getDeclaredFields();
				for (Field field : fields) {
					set.add(field.getName());
				}
				map.put(entry.getKey(), set);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return map;
	}

	/**
	 * 将数据格式化成bean对应的数据， Object等直接返回原值
	 * 
	 * @param str
	 * @param field
	 * @return
	 */
	public static Object getFieldFromHbase(String str, Field field) {
		Object obj = str;
		if (Date.class.equals(field.getType())) {
			obj = TimeUtils.getDateFromYYYYMMDD_hhmmss(str);
		} else if (Integer.class.equals(field.getType())) {
			try {
				obj = Integer.parseInt(str);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		} else if (Long.class.equals(field.getType())) {
			try {
				obj = Long.parseLong(str);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		} else if (Short.class.equals(field.getType())) {
			try {
				obj = Short.parseShort(str);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		return obj;
	}
	
	/**
	 * 组装put
	 * @param mp
	 * @return
	 */
	public static Put getPut(Model_Base mp){
		if (null == mp.getUuid()) {
			mp.setUuid(GenUuid.GetUuid());
		}
		Put put = new Put(mp.getUuid().getBytes());
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
								put.addColumn(family.getBytes(), property.getBytes(), value.getBytes());
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
									put.addColumn(family.getBytes(), property.getBytes(), value.getBytes());
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
		return put;
		
	}

	public static void main(String[] args) {
		System.out.println(getHbaseStringFromField(1));
	}
}
