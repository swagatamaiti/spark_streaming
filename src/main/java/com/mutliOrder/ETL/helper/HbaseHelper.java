package com.mutliOrder.ETL.helper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.mutliOrder.ETL.constant.Constants;
import com.mutliOrder.common.conf.ConfigFactory;


public class HbaseHelper {
	
	private static Connection connection = null;
	private static Admin admin = null;
	private static String zk = null;
	private static final Map<String,Table> tableMap = new HashMap<String,Table>();
		
	static{	
		try {
			zk = ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.ZK_LIST);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static HbaseHelper instance = null;
	
	public static HbaseHelper getInstance() throws IOException {
		if(instance == null) {
			synchronized(HbaseHelper.class) {
				if(instance == null) {
					instance = new HbaseHelper();
				}
			}
		}
		return instance;
	} 
	
	private HbaseHelper() throws IOException {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", zk);
		connection = ConnectionFactory.createConnection(conf);
		admin = connection.getAdmin();
	}
	
	 public void close() throws IOException {
		 Iterator<Entry<String, Table>> it = tableMap.entrySet().iterator();
		 while(it.hasNext()){
			 it.next().getValue().close();
		 }
		 tableMap.clear();
		 if(admin!=null){
			 admin.close();
		 }
		if(connection!=null){
			connection.close();
		}
		instance=null;
		
	 }
	 
	 private Table getTable(String tableName) throws IOException{
		 Table _table = null;
		 if(connection.isAborted()||connection.isClosed()){
			 close();
			 getInstance();
		 }
		 if(tableMap.containsKey(tableName)){
				_table = tableMap.get(tableName);
			}else{
				_table = connection.getTable(TableName.valueOf(tableName));
				tableMap.put(tableName, _table);
			}
		 return _table;
	 }
	
	public void createTable(String tablename, String cf){
		try {
			if(admin.tableExists(TableName.valueOf(tablename))){
//				System.out.println("table :"+ tablename +"exsit");
			}else{
				HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tablename));
				descriptor.addFamily(new HColumnDescriptor(cf.getBytes()));
				admin.createTable(descriptor);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	public void majorCompact(String table) throws IOException {
		admin.majorCompact(TableName.valueOf(table));
	}
	
	public void truncateTable(String table) throws IOException{
		admin.disableTable(TableName.valueOf(table));
		admin.truncateTable(TableName.valueOf(table), true);
	}
	
	public void renameTable(String oldTableName, String newTableName) throws IOException{
		
		String snapshotName = "rename_tmp_table";
		admin.disableTables(oldTableName);
		//admin.snapshot(snapshotName, TableName.valueOf(oldTableName));
		admin.cloneSnapshot(snapshotName, TableName.valueOf(newTableName));
	    admin.deleteSnapshot(snapshotName);
	    admin.deleteTable(TableName.valueOf(oldTableName));
	}
	
	
	public boolean checkTableExist(String tablename) throws IOException {
		
		return admin.tableExists(TableName.valueOf(tablename));
	}
	
	public void deleteTable(String tablename){
		try {
			
			if (admin.tableExists(TableName.valueOf(tablename))) {
				admin.disableTable(TableName.valueOf(tablename));
				admin.deleteTable(TableName.valueOf(tablename));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	public boolean insertAndCheck(String tableName,String rowKey,String family,HashMap<String, String> clMap, 
			String chfamily, String chquailifer, String chvalue){
		
		Table _table = null;
		boolean res = true;
		
		try {
			_table = getTable(tableName);
			Put put = new Put(rowKey.getBytes());
			for(String qkey : clMap.keySet()){
				String value = clMap.get(qkey);
				put.addColumn(family.getBytes(), qkey.getBytes(), value.getBytes()) ;
			}
			if(null == chvalue || 0 == chvalue.length()){
				res = _table.checkAndPut(rowKey.getBytes(), chfamily.getBytes(), chquailifer.getBytes(), null, put);
			}else{
				res = _table.checkAndPut(rowKey.getBytes(), chfamily.getBytes(), chquailifer.getBytes(), chvalue.getBytes(), put);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 		
		return res;
	}
	
	public void insert(String tableName, String rowKey, String family,
			String quailifer, String value) {
		Table _table = null;
		try {
			_table = getTable(tableName);
			Put put = new Put(rowKey.getBytes());
			put.addColumn(family.getBytes(), quailifer.getBytes(), value.getBytes()) ;
			_table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	public void insert(String tableName,String rowKey,String family,HashMap<String, String> clMap){
		Table _table = null;
		try {
			_table = getTable(tableName);
			Put put = new Put(rowKey.getBytes());
			for(String qkey : clMap.keySet()){
				String value = clMap.get(qkey);
				put.addColumn(family.getBytes(), qkey.getBytes(), value.getBytes()) ;
			}
			_table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(clMap.toString());
		} 
	}
	
	public boolean checkRowkeyExist(String tableName,String rowkey) {
		Table _table = null;
		boolean exist = false;
		try {
			_table = getTable(tableName);
			Get get = new Get(rowkey.getBytes());
			exist = _table.exists(get);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return exist;
	}
	
	public void insert(String tableName,HashMap<String, HashMap<String, HashMap<String, String>>> kvMap)
	{	
		Table _table = null;
		try {
			_table = getTable(tableName);
			List<Put> puts = new ArrayList<Put>();
			
			for (String rowKey : kvMap.keySet()){
				HashMap<String, HashMap<String, String>> rValues = kvMap.get(rowKey);
				Put p = new Put(Bytes.toBytes(rowKey));
				for (Entry<String, HashMap<String, String>> entry : rValues.entrySet()) {
					String family = entry.getKey();
					HashMap<String, String> fValues = entry.getValue();
					for(String qkey : fValues.keySet()){
						String value = fValues.get(qkey);
						p.addColumn(family.getBytes(), qkey.getBytes(), value.getBytes()) ;
					}
				}
				puts.add(p);
			}
			_table.put(puts);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	
	public void insert(String tableName,String family, Map<String, Map<String, String>> kvMap)
	{	
		Table _table = null;
		try {
			_table = getTable(tableName);
			List<Put> puts = new ArrayList<Put>();
			
			for (Entry<String, Map<String, String>> kvEntry : kvMap.entrySet()){
				if(kvEntry==null || kvEntry.getValue().isEmpty()){
					continue;
				}
				Put put = new Put(kvEntry.getKey().getBytes());
				for(Entry<String,String> entry:kvEntry.getValue().entrySet()){
					put.addColumn(family.getBytes(), entry.getKey().getBytes(), entry.getValue().getBytes());
				}
				puts.add(put);
			}
			if(!puts.isEmpty()){
				_table.put(puts);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	public long incrementColumeValue(String tableName, String rowKey, String family,String quailifer, Long value){
		
		Table _table = null;
		long res = 0;
		try {
			_table = getTable(tableName);
			res = _table.incrementColumnValue(rowKey.getBytes(), family.getBytes(), quailifer.getBytes(), value);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return res;
	}
	
	public void incrementColumeValues(String tableName, String rowKey, String family[],String[] quailifer, Long value[]){
		
		Table _table = null;
		try {
			_table = getTable(tableName);
			if (quailifer != null && 
					value != null && 
					family != null &&
					family.length == quailifer.length &&
					quailifer.length == value.length) {
				Increment increment = new Increment(rowKey.getBytes());
				
				for(int i = 0; i < quailifer.length; i++){
					increment.addColumn(family[i].getBytes(), quailifer[i].getBytes(), value[i]);
				}
				_table.increment(increment);
			}
		} catch (IOException e) { 
			e.printStackTrace();
		} 
	}
	
	public void getOneRow(String tableName, String rowKey,QueryCallback callback) throws Exception {

		List<Result> list = new ArrayList<Result>();
		Result rsResult = null;
		Table _table = null;
		try {
			_table = getTable(tableName);
			Get get = new Get(rowKey.getBytes()) ;
			rsResult = _table.get(get) ;
			list.add(rsResult);
			callback.process(list);
		} catch (Exception e) {
			e.printStackTrace() ;
		} 

	}
	
	public Result getOneRow(String tableName, String rowKey) throws Exception {
		Result rsResult = null;
		Table _table = null;
		try {
			_table = getTable(tableName);
			Get get = new Get(rowKey.getBytes()) ;
			rsResult = _table.get(get) ;
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		return rsResult;

	}
	
	public String getOneRow(String tableName, String rowKey,String family, String qualifier) throws Exception {
		String result = null;
		Result rsResult = null;
		Table _table = null;
		try {
			_table = getTable(tableName);
			Get get = new Get(rowKey.getBytes());
			get.addColumn(family.getBytes(), qualifier.getBytes());
			rsResult = _table.get(get);
			if(null!=rsResult && !rsResult.isEmpty() && rsResult.size() > 0){
				result = Bytes.toString(rsResult.value());
			}
		} catch (Exception e) {
			e.printStackTrace() ;
		} 
		return result;
	}
	
	public void getRows(String tableName, String rowKeyLike, QueryCallback callback) throws Exception {
		
		Table _table = null;
		List<Result> list = new ArrayList<Result>();
		try {
			_table = getTable(tableName);
			Scan scan = new Scan();
			if (rowKeyLike != null) {
				PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
				scan.setFilter(filter);
			}
			ResultScanner scanner = _table.getScanner(scan) ;
			for (Result rs : scanner) {
				list.add(rs) ;
			}
			callback.process(list);
		} catch (Exception e) {
			e.printStackTrace() ;
		} 
	}
	
	public void getRows(String tableName, String rowKeyLike , String famls[], String cols[], QueryCallback callback) throws Exception {
		
		Table _table = null;
		List<Result> list = new ArrayList<Result>();
		try {
			_table = getTable(tableName);
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			for(String faml:famls){
				for (int i = 0; i < cols.length; i++) {
					scan.addColumn(faml.getBytes(), cols[i].getBytes()) ;
				}
			}
			scan.setFilter(filter);
			ResultScanner scanner = _table.getScanner(scan) ;
			for (Result rs : scanner) {
				list.add(rs) ;
			}
			callback.process(list);
		} catch (Exception e) {
			e.printStackTrace() ;
		} 
		
	}
	
	public void deleteRow(String tableName,String row){

		Table _table = null;
		try {
			Delete delete = new Delete(row.getBytes());
			_table = getTable(tableName);
			_table.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		}
	
	}
	
	public void deleteRows(String tableName,String[] rows){

		Table _table = null;
		if (rows != null && rows.length > 0) {
			List<Delete> list = new ArrayList<Delete>();
			
			for (String row : rows) {
				Delete delete = new Delete(row.getBytes());
				list.add(delete);
			}
			try {
				_table = getTable(tableName);
				_table.delete(list);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public void deleteRows(String tableName,Object[] rows){

		Table _table = null;
		if (rows != null && rows.length > 0) {
			List<Delete> list = new ArrayList<Delete>();
			
			for (Object row : rows) {
				Delete delete = new Delete(String.valueOf(row).getBytes());
				list.add(delete);
			}
			try {
				_table = getTable(tableName);
				_table.delete(list);
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
		
	}
	public static void main(String[] args) throws IOException, Exception {
		System.out.println(getInstance().checkRowkeyExist("App_Comment", "123"));
	}
	public static interface QueryCallback {
		
		void process(List<Result>  resultList) throws Exception;
		
	}
}