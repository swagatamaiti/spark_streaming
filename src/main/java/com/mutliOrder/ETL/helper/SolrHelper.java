package com.mutliOrder.ETL.helper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;

import com.mutliOrder.ETL.constant.Constants;
import com.mutliOrder.common.conf.ConfigFactory;

/**
 * solr查询工具
 */
public class SolrHelper {

	private static CloudSolrServer server = null;
	private static String solrZkHost = null;
	private static int MAX_QUERY_LIMIT = 200;
	private static Logger logger;

	static{
		try {
			solrZkHost = ConfigFactory.getInstance()
					.getConfigProperties(Constants.DATA_CONFIG_PATH).getProperty(Constants.SOLR_ZK_HOST);
			MAX_QUERY_LIMIT = Integer.parseInt(ConfigFactory.getInstance().getConfigProperties(Constants.DATA_CONFIG_PATH)
					.getProperty(Constants.MAX_QUERY_LIMIT));
			logger = Logger.getLogger(SolrHelper.class);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static SolrHelper instance = null;

	public static SolrHelper getInstance() throws IOException {
		if(instance == null) {
			synchronized(SolrHelper.class) {
				if(instance == null) {
					instance = new SolrHelper();
				}
			}
		}
		return instance;
	}
	
	public CloudSolrServer getServer(){
		return server;
	}

	private SolrHelper() throws IOException {
		server = new CloudSolrServer(solrZkHost);
	}

	 public void close() throws IOException {
		 server.shutdown();
	 }
	 /**
	  * 根据给定的query语句到collection中查询field字段，single为true则返回一条数据，false返回多条
	  * @param collection
	  * @param query
	  * @param field
	  * @param single
	  * @return
	 * @throws SolrServerException 
	  */
	 public QueryResponse getDoc(String collection,String query,String field,Boolean single,String sortField,ORDER order) throws SolrServerException{

		SolrQuery solrQuery = new SolrQuery();
		solrQuery.set("collection", collection);
		if(sortField!=null&&order!=null){
			solrQuery.addSort(sortField, order);
		}
		if(null != field){
			solrQuery.setFields(field);
		}
		if(single){
			solrQuery.setRows(1);
		}
		else{
			solrQuery.setRows(MAX_QUERY_LIMIT);
		}
		solrQuery.setQuery(query);
		QueryResponse queryResponse = null;
		queryResponse = server.query(solrQuery);
		return queryResponse;
	 }


    /**
     * 从solr查询文档，最大条数为maxRows
     *
     * @param collection 表名
     * @param query      查询语句
     * @param maxRows    最大条数
     * @return the query response
     */
    public QueryResponse getDocAll(String collection, String query, int maxRows) {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.set("collection", collection);
        solrQuery.setQuery(query);
        solrQuery.setRows(maxRows);

        QueryResponse queryResponse = null;
        try {
            queryResponse = server.query(solrQuery);
        } catch (Exception e) {
            logger.error("query:" + query + ";collection:" + collection);
        }
        return queryResponse;

    }
    
    public void addDocs(Collection<SolrInputDocument> docs,String collection) {
    	try {
            server.setDefaultCollection(collection);
            server.add(docs);
            server.commit();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException, SolrServerException {
    	SolrInputDocument sid =new SolrInputDocument();
    	sid.addField("id", "123");
//    	sid.addField("word_rank", "1234");
    	sid.addField("name", "1234");
//    	sid.addField("_version_", 1232333l);
    	List<SolrInputDocument> list = new ArrayList<SolrInputDocument>();
    	list.add(sid);
		System.out.println(getInstance().getDoc("App_Base", "AppId:1167006598", "id", true, "batch", ORDER.desc));;
	}
   
}
