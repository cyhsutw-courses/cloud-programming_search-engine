package cp.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;

public class App {

	/* API can be found here:
		https://hbase.apache.org/apidocs/
	*/
	
	public static void main(String[] args) throws IOException {
		Configuration conf = null;
		Connection conn = null;
		Admin admin = null;
		
		String[] rawQuery = args[0].toLowerCase().trim().split("\\s");
		if (rawQuery.length == 0){
			System.exit(0);
		}
		
		Map<String, Integer> queryTermFreq = new HashMap<>();
		
		for (String token : rawQuery) {
			Integer count = queryTermFreq.get(token);
			if (count == null) {
				count = 1;
			} else {
				count = count + 1;
			}
			
			queryTermFreq.put(token, count);
		}
		
		List<String> queryTokenOrder = new ArrayList<>(queryTermFreq.keySet()); 
		
		Double[] qVec = new Double[queryTokenOrder.size()];
		
		for (String token : queryTokenOrder) {
			Double tf = (double) queryTermFreq.get(token).intValue();
			qVec[queryTokenOrder.indexOf(token)] = tf;
		}
		
		try {
			conf = HBaseConfiguration.create();
			conn = ConnectionFactory.createConnection(conf);
			admin = conn.getAdmin();
			
			Table docIdTitleMappingTable = conn.getTable(TableName.valueOf("s103062512:map".getBytes()));
			
			Table invertedIndexTable = conn.getTable(TableName.valueOf("s103062512:invertedindex".getBytes()));
			
			
			Set<String> validDocIds = null;
			Map<String, Double[]> docVecMap = new HashMap<>();
			
			// (doc, (term, offsets)) 
			Map<String, Map<String, Integer[]>> docTermOccMap = new HashMap<>();
			
			Map<String, Double> termIdfMap = new HashMap<>();
			
			for (String token : queryTokenOrder) {
				Get get = new Get(token.getBytes());
				Result result = invertedIndexTable.get(get);
				
				if (result.isEmpty()) {
					System.out.println("No page found.");
					
					docIdTitleMappingTable.close();
					invertedIndexTable.close();
					admin.close();
					conn.close();
					
					System.exit(1);
				}
				
				int vecIndex = queryTokenOrder.indexOf(token);
				
				double invertedDocFreq = Double.parseDouble(new String(result.getValue("idf".getBytes(), "".getBytes())));
				String occurrenceString = new String(result.getValue("occurrences".getBytes(), "".getBytes()));
				
				termIdfMap.put(token, invertedDocFreq);
				
				String[] occurrences = occurrenceString.split("/");
				
				Set<String> ids = new HashSet<>();
				for (String occ : occurrences) {
					String[] comps = occ.split(";");
					String docId = comps[0];
					Double tf = Double.parseDouble(comps[1]);
					String[] occStrings = comps[2].split(",");
					List<Integer> occInts = new ArrayList<>();
					
					for (String t : occStrings) {
						occInts.add(Integer.parseInt(t));
					}
					
					ids.add(docId);
					
					Map<String, Integer[]> termOcc = docTermOccMap.get(docId);
					
					if (termOcc == null) {
						termOcc = new HashMap<String, Integer[]>();
					}
					
					termOcc.put(token, occInts.toArray(new Integer[occInts.size()]));
					docTermOccMap.put(docId, termOcc);
					
					
					Double[] vec = docVecMap.get(docId);
					if (vec == null) {
						vec = new Double[queryTokenOrder.size()];
					}
					
					vec[vecIndex] = invertedDocFreq * tf;
					docVecMap.put(docId, vec);
				}
				
				if (validDocIds == null) {
					validDocIds = new HashSet<>();
					validDocIds.addAll(ids);
				} else {
					validDocIds.retainAll(ids);
				}
				
			}
			
			if (validDocIds.isEmpty()) {
				System.out.println("No page found.");
				
				docIdTitleMappingTable.close();
				invertedIndexTable.close();
				admin.close();
				conn.close();
				
				System.exit(1);
			}
			
			
			for (String token : queryTokenOrder) {
				int index = queryTokenOrder.indexOf(token);
				Double idf = termIdfMap.get(token);
				Double tf = qVec[index];
				qVec[index] = tf * idf;
			}
			
			Map<String, Double> docScoreMap = new HashMap<>();
			
			Table pageRankTable = conn.getTable(TableName.valueOf("s103062512:pagerank".getBytes()));
			
			for (String id : validDocIds) {
				Double[] myVec = docVecMap.get(id);
				double cosSim = cosSim(qVec, myVec);
				
				Get get = new Get(id.getBytes());
				Result result = pageRankTable.get(get);
				
				double rank = Double.parseDouble(new String(result.getValue("score".getBytes(), "".getBytes())));
				
				double score = cosSim * 0.7 + rank * 0.3;
				
				docScoreMap.put(id, score);
				
			}
			
			List<Entry<String, Double>> docScore = new ArrayList<>(docScoreMap.entrySet());
			Collections.sort(docScore, new Comparator<Entry<String, Double>>() {
	            @Override
	            public int compare(Entry<String, Double> entry1, Entry<String, Double> entry2) {
	                return Double.compare(entry2.getValue(), entry1.getValue());
	            }
	        });
			
			docScore = docScore.subList(0, 10);
			
			List<Entry<String, Double>> idfScores = new ArrayList<>(termIdfMap.entrySet());
			Collections.sort(idfScores, new Comparator<Entry<String, Double>>() {
	            @Override
	            public int compare(Entry<String, Double> entry1, Entry<String, Double> entry2) {
	                return Double.compare(entry2.getValue(), entry1.getValue());
	            }
	        });
			
			
			Table contentTable = conn.getTable(TableName.valueOf("s103062512:doc".getBytes()));
			Table mapTable = conn.getTable(TableName.valueOf("s103062512:map".getBytes()));
			
			for (Entry<String, Double> ds : docScore) {
				Map<String, Integer[]> offs = docTermOccMap.get(ds.getKey());
				List<Integer> imp = new ArrayList<>();
				for (Entry<String, Double> idfe : idfScores) {
					Integer[] one = offs.get(idfe.getKey());
					for (Integer iii : one) {
						imp.add(iii);
					}
					if (imp.size() >= 3) {
						break;
					}
				}
				
				int maxIndex = imp.size() >= 3 ? 3 : imp.size();
				
				imp = imp.subList(0, maxIndex);
				
				String docId = ds.getKey();
				
				Get titleGet = new Get(docId.getBytes());
				Result titleGetResult = mapTable.get(titleGet);
				String title = new String(titleGetResult.getValue("title".getBytes(), "".getBytes()));
				
				Get contentGet = new Get(docId.getBytes());
				Result contentGetResult = contentTable.get(contentGet);
				String content = new String(contentGetResult.getValue("content".getBytes(), "".getBytes()));
				
				System.out.println(title + " - " + ds.getValue().toString());
				System.out.println("------------------------------------------");
				
				for (Integer index : imp) {
					int upper = index + 20;
					int lower = index - 20;
					
					upper = Math.min(content.length() - 1, upper);
					lower = Math.max(0, lower);
					
					System.out.println(content.substring(lower, upper));
				}
				System.out.println("\n");
			}
			
			mapTable.close();
			contentTable.close();
			pageRankTable.close();
			docIdTitleMappingTable.close();
			invertedIndexTable.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (admin != null) {
				admin.close();
			}

			if (conn != null) {
				conn.close();
			}
		}
	}
	
	private static double cosSim(Double[] vec1, Double[] vec2) {
		double len1 = 0.0;
		
		for (double v : vec1) {
			len1  = len1 + v * v;
		}
		
		len1 = Math.sqrt(len1);
		
		double len2 = 0.0;
		
		for (double v : vec2) {
			len2  = len2 + v * v;
		}
		
		len2 = Math.sqrt(len2);
		
		
		double innerProduct = 0.0;
		
		for (int i = 0; i < vec1.length; i = i + 1) {
			innerProduct = innerProduct + (vec1[i] * vec2[i]);
		}
		
		return innerProduct / (len1 * len2);
	}
}