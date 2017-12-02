package io.bittiger.ads;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;

import net.spy.memcached.MemcachedClient;


public class QueryParser {
	private static QueryParser instance = null;
	
	protected QueryParser() {
		
	}
	public static QueryParser getInstance() {
	      if(instance == null) {
	         instance = new QueryParser();
	      }
	      return instance;
    }
	public List<String> QueryUnderstand(String query) {
		List<String> tokens = Utility.cleanedTokenize(query);
		return tokens;
	}
	
	//nike running shoes
		private void QueryRewriteHelper(int index, int len, ArrayList<String> queryTermsTemp,List<List<String>> allSynonymList, List<List<String>> res) {
			if(index == len) {
				res.add(queryTermsTemp);
				return;
			}
			List<String> synonyms = allSynonymList.get(index);
			for(int i = 0; i < synonyms.size();i++) {			
				ArrayList<String> queryTerms = (ArrayList<String>) queryTermsTemp.clone();
				queryTerms.add(synonyms.get(i));
				QueryRewriteHelper(index + 1,len,queryTerms,allSynonymList,res);
			}	
		}
	
	//get offline query rewrite
	public List<List<String>> OfflineQueryRewrite(String query, String memcachedServer,int memcachedPortal) {
		List<List<String>> res = new ArrayList<List<String>>();
		List<String> tokens = Utility.cleanedTokenize(query);
		String query_key = Utility.strJoin(tokens, "_");
		try {
			MemcachedClient cache = new MemcachedClient(new InetSocketAddress(memcachedServer, memcachedPortal));
			if(cache.get(query_key) instanceof List) {
				@SuppressWarnings("unchecked")
				List<String>  synonyms = (ArrayList<String>)cache.get(query_key);
				for(String synonym : synonyms) {
					List<String> token_list = new ArrayList<String>();
					String[] s = synonym.split("_");
					for(String w : s) {
						token_list.add(w);
					}
					res.add(token_list);
				}			
			}
			else {
				res.add(tokens);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	 
		return res;
	}
	
	
	//construct query rewrite offline
	public void OfflineQueryRewriteCreat(File synonymFile,File ads_input_file){
		FileReader fileR;
		try {
			fileR = new FileReader(synonymFile);
			BufferedReader br = new BufferedReader(fileR); 
			Hashtable<String,List<String>> synonymTable = new Hashtable<String,List<String>>();
			String line=null;
			//construct synonyms hashtable
			while((line=br.readLine())!=null){
				JSONObject synonyms= JSONObject.parseObject(line); 
				String key = synonyms.getString("word");
				List<String> value = synonyms.getJSONArray("synonyms").toJavaList(String.class);
				synonymTable.put(key,value);
			}
			
			// read input ads file 
			FileReader fileA = new FileReader(ads_input_file);
			BufferedReader br_ads = new BufferedReader(fileA); 
			List<List<String>> queryTerms = new ArrayList<List<String>>();
			line =null;
			Map map = new HashMap();
			while((line=br_ads.readLine())!=null){
				JSONObject query = JSONObject.parseObject(line);
				String adsquery = query.getString("query");
				//dedupe query
				if(map.get(adsquery)==null){
					map.put(adsquery, 1);
				}else{
					continue;
				}
				String[] terms= query.getString("query").split(" ");
				List termList = Arrays.asList(terms);
				queryTerms.add(termList);
			}
			
			//construct query rewrite
			for(List queryterms:queryTerms){
				OnlineCreateQueryRewrite(queryterms,"127.0.0.1",11212,synonymTable);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//construct query rewrite & write to memcache
	public void OnlineCreateQueryRewrite(List<String> queryTerms,String memcachedServer, int synonymMemcachedPortal,Hashtable synonyms) {
		List<List<String>> res = new ArrayList<List<String>>();
		List<List<String>> resTemp = new ArrayList<List<String>>();
		List<List<String>> allSynonymList = new ArrayList<List<String>>();
		StringBuilder rewriteString =null;
		MemcachedClient cache = null;
		try {
			for(String queryTerm:queryTerms) {
				if(synonyms.get(queryTerm) instanceof List) {
					List<String>  synonymList = (List<String>)synonyms.get(queryTerm);
					allSynonymList.add(synonymList);	
				} else {
					List<String>  synonymList = new ArrayList<String>();
					synonymList.add(queryTerm);
					allSynonymList.add(synonymList);
				}	
			}
			int len = queryTerms.size();
			System.out.println("len of queryTerms = " + len);
			ArrayList<String> queryTermsTemp = new ArrayList<String>();
			QueryRewriteHelper(0, len, queryTermsTemp,allSynonymList,resTemp);	

			//dedupe
			Set<String> uniquueQuery = new HashSet<String>();
			for(int i = 0;i < resTemp.size();i++) {
				String hash = Utility.strJoin(resTemp.get(i), "_");
				if(uniquueQuery.contains(hash)) {
					continue;
				}
				uniquueQuery.add(hash);
				Set<String> uniquueTerm = new HashSet<String>();
				for(int j = 0;j < resTemp.get(i).size();j++) {
					String term = resTemp.get(i).get(j);
					if(uniquueTerm.contains(term)) {
						break;
					}
					uniquueTerm.add(term);
				}
				if (uniquueTerm.size() == len) {
					res.add(resTemp.get(i));
				}
			}
			
			//generate query rewrite list
			rewriteString = new StringBuilder();
			for(int i = 0;i < res.size();i++) {
				System.out.println("synonym");
				StringBuffer sb = new StringBuffer();
				for(int j = 0;j < res.get(i).size();j++) {
					System.out.println("query term = " + res.get(i).get(j));
					sb.append(res.get(i).get(j));
					if(j<res.get(i).size()-1){
						sb.append("_");
					}
				}			
				rewriteString.append(sb.toString());
				if(i<res.size()-1){
					rewriteString.append(",");
				}
			}
			
			// write genrated query rewrite to memcache
			cache = new MemcachedClient(new InetSocketAddress(memcachedServer, synonymMemcachedPortal));
			StringBuffer rawQuery = new StringBuffer();
			for(String term:queryTerms){
				rawQuery.append("_").append(term);
			}
			String queryRwriteKey =  rawQuery.substring(1);
			String queryRwriteValue = rewriteString.toString();
			
			cache.set(queryRwriteKey,0,queryRwriteValue);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			cache.shutdown();
		}
		
	}
	
    //construct query rewrite online
	public List<List<String>> OnlineQueryRewrite(List<String> queryTerms,String memcachedServer, int synonymMemcachedPortal) {
		List<List<String>> res = new ArrayList<List<String>>();
		List<List<String>> resTemp = new ArrayList<List<String>>();
		List<List<String>> allSynonymList = new ArrayList<List<String>>();
		try {
			MemcachedClient cache = new MemcachedClient(new InetSocketAddress(memcachedServer, synonymMemcachedPortal));
			for(String queryTerm:queryTerms) {
				if(cache.get(queryTerm) instanceof List) {
					List<String>  synonymList = (List<String>)cache.get(queryTerm);
					allSynonymList.add(synonymList);	
				} else {
					List<String>  synonymList = new ArrayList<String>();
					synonymList.add(queryTerm);
					allSynonymList.add(synonymList);
				}	
			}
			int len = queryTerms.size();
			System.out.println("len of queryTerms = " + len);
			ArrayList<String> queryTermsTemp = new ArrayList<String>();
			QueryRewriteHelper(0, len, queryTermsTemp,allSynonymList,resTemp);	

			//dedupe
			Set<String> uniquueQuery = new HashSet<String>();
			for(int i = 0;i < resTemp.size();i++) {
				String hash = Utility.strJoin(resTemp.get(i), "_");
				if(uniquueQuery.contains(hash)) {
					continue;
				}
				uniquueQuery.add(hash);
				Set<String> uniquueTerm = new HashSet<String>();
				for(int j = 0;j < resTemp.get(i).size();j++) {
					String term = resTemp.get(i).get(j);
					if(uniquueTerm.contains(term)) {
						break;
					}
					uniquueTerm.add(term);
				}
				if (uniquueTerm.size() == len) {
					res.add(resTemp.get(i));
				}
			}
			//debug
			for(int i = 0;i < res.size();i++) {
				System.out.println("synonym");
				for(int j = 0;j < res.get(i).size();j++) {
					System.out.println("query term = " + res.get(i).get(j));
				}			
			}
						
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return res;
	}
}
