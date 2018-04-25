package com.mutliOrder.DataBean;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public interface Constants {
	Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	Gson exposeGson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
	String LOG4J_CONFIG_PATH="/log.properties";
	long AN_HOURS_MILLIS = 60*60*1000;
	long EIGHT_HOURS_MILLIS = 8*AN_HOURS_MILLIS;
	long ONE_DAY_MILLIS = 24*AN_HOURS_MILLIS;
}
