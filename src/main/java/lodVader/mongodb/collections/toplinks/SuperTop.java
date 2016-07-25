package lodVader.mongodb.collections.toplinks;

import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import lodVader.LODVaderProperties;
import lodVader.mongodb.DBSuperClass2;

public class SuperTop extends DBSuperClass2{

	public static String AMOUNT = "amount";

	public static String DETAILS = "details";

	public static String LINK = "link";

	public static String SOURCE_DISTRIBUTION_ID = "sourceDistributionID";

	public static String TARGET_DISTRIBUTION_ID = "targetDistributionID";
	
	public SuperTop(String collectionName) {
		super(collectionName);
	}

	public void saveAll(HashMap<String, Integer> values, int sourceDistributionID, int targetDistributionID) {
		
		removeAll(sourceDistributionID, targetDistributionID);

		DBCollection collection = DBSuperClass2.getDBInstance().getCollection(COLLECTION_NAME);
		BasicDBObject mainDoc = new BasicDBObject();
		mainDoc.append(SOURCE_DISTRIBUTION_ID, sourceDistributionID);
		mainDoc.append(TARGET_DISTRIBUTION_ID, targetDistributionID);
		BasicDBList innerList = new BasicDBList();

		for (String s : topNKeys(values, LODVaderProperties.TOP_N_LINKS).keySet()) {
			BasicDBObject innerDoc = new BasicDBObject();
			innerDoc.append(AMOUNT, values.get(s));
			innerDoc.append(LINK, s);
			innerList.add(innerDoc);
		}
		if(innerList.size()>0){
		mainDoc.append(DETAILS, innerList);

		collection.insert(mainDoc);
		}
	}

	public HashMap<String, Integer> topNKeys(final HashMap<String, Integer> map, int n) {
		PriorityQueue<String> topN = new PriorityQueue<String>(n, new Comparator<String>() {
			public int compare(String s1, String s2) {
				return Double.compare(map.get(s1), map.get(s2));
			}
		});

		for (String key : map.keySet()) {
			if (topN.size() < n)
				topN.add(key);
			else if (map.get(topN.peek()) < map.get(key)) {
				topN.poll();
				topN.add(key);
			} 
		}

		HashMap<String, Integer> returnValue = new HashMap<String, Integer>();
		for (String s : topN) {
			returnValue.put(s, map.get(s));
		}

		return returnValue;
	}

	public void removeAll(int distributionSourceID, int targetDistributionID) {
		DBCollection collection = DBSuperClass2.getDBInstance().getCollection(COLLECTION_NAME);
		
		BasicDBList list = new BasicDBList();
		list.add(new BasicDBObject(SOURCE_DISTRIBUTION_ID, distributionSourceID));
		list.add(new BasicDBObject(TARGET_DISTRIBUTION_ID, targetDistributionID));

		collection.remove(new BasicDBObject("$and", list));
	}

}
