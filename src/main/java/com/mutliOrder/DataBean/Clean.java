package com.mutliOrder.DataBean;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.google.gson.JsonSyntaxException;
import com.mutliOrder.DataBean.bean.base.BaseBean;
import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.common.Class.ClassUtils;
import com.mutliOrder.common.conf.ConfProperties;
import com.mutliOrder.common.conf.ConfigFactory;

public class Clean {
	private static Map<String, Class<?>> map = null;
	private static Logger logger;
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
		logger = Logger.getLogger(Clean.class);
	}
	static {
		try {
			map = ClassUtils.getClassesExtends("com.multiOrder.DataBean.bean", Model_Base.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Model_Base process(String json) {
		Model_Base base = null;
//		logger.error(json);
		try {
			BaseBean baseBean = Constants.gson.fromJson(json, BaseBean.class);
			if(map!=null){
				Class<?> clas = map.get(baseBean.getContentType());
				if (baseBean.getContent() != null&&clas!=null) {
					try {
						base = (Model_Base) Constants.gson.fromJson(Constants.gson.toJson(baseBean.getContent()),
								clas);
						base.setFetchTaskId(baseBean.getFetchTaskId());
						base.setFetchTime(baseBean.getFetchTime());
						base.setAppleStoreFront(baseBean.getAppleStoreFront());
						base.clean();
					} catch (JsonSyntaxException e) {
						//TODO
						logger.error(json);
						logger.error(e);
						e.printStackTrace();
					}
				}else{
					//TODO
					logger.error(json);
					logger.error("clas:"+clas+" getContent:"+baseBean.getContent());
				}

			}else{
				logger.error(json);
				logger.error("map==null");
			}
			
		} catch (JsonSyntaxException e) {
			logger.error(json);
			logger.error(e);
			e.printStackTrace();
		}
		return base;
	}
}
