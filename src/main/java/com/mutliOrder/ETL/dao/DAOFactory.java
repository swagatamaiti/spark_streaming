package com.mutliOrder.ETL.dao;

import com.mutliOrder.ETL.dao.impl.CommonPageDaoImpl;

public class DAOFactory {
	private static CommonPageDao cpd = null;
	public static CommonPageDao getCommonPageDao() {
		if(cpd==null){
			synchronized(DAOFactory.class){
				if(cpd==null){
					cpd = new CommonPageDaoImpl();
				}
			}
		}
		return cpd;
	}
}
