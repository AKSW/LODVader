package lodVader.mongodb.collections.toplinks;

import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import lodVader.LODVaderProperties;
import lodVader.mongodb.DBSuperClass;

public class SuperTop {

	public static String COLLECTION_NAME = null;

	public static String AMOUNT = "amount";

	public static String LINK = "link";

	public static String SOURCE_DISTRIBUTION_ID = "sourceDistributionID";

	public static String TARGET_DISTRIBUTION_ID = "targetDistributionID";
	
	public SuperTop(String collectionName) {
		this.COLLECTION_NAME = collectionName;
	}

	public void saveAll(HashMap<String, Integer> values, int sourceDistributionID, int targetDistributionID) {
		
		removeAll(sourceDistributionID, targetDistributionID);

		DBCollection collection = DBSuperClass.getInstance().getCollection(COLLECTION_NAME);

		for (String s : topNKeys(values, LODVaderProperties.TOP_N_LINKS).keySet()) {
			BasicDBObject insert = new BasicDBObject();
			insert.append(AMOUNT, values.get(s));
			insert.append(LINK, s);
			insert.append(SOURCE_DISTRIBUTION_ID, sourceDistributionID);
			insert.append(TARGET_DISTRIBUTION_ID, targetDistributionID);
			collection.insert(insert);
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
		DBCollection collection = DBSuperClass.getInstance().getCollection(COLLECTION_NAME);
		
		BasicDBList list = new BasicDBList();
		list.add(new BasicDBObject(SOURCE_DISTRIBUTION_ID, distributionSourceID));
		list.add(new BasicDBObject(TARGET_DISTRIBUTION_ID, targetDistributionID));

		collection.remove(new BasicDBObject("$and", list));
	}

}
