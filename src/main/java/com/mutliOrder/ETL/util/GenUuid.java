package com.mutliOrder.ETL.util;

import java.util.UUID;

public class GenUuid {
	/**
	 * 返回uuid
	 * @return
	 */
	public static String GetUuid() {
		UUID uuid = UUID.randomUUID();
		
		String str = uuid.toString().replace("-", "");
		return str; 
	}

}
