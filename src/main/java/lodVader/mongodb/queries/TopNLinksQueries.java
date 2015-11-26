package lodVader.mongodb.queries;

import java.util.LinkedHashMap;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.toplinks.SuperTop;

public class TopNLinksQueries {

	public LinkedHashMap<String, Integer> getTopNLinks(int dataset1, int dataset2, String collectionName) {

		LinkedHashMap<String, Integer> result = new LinkedHashMap<String, Integer>();

		
		DBCollection collection = DBSuperClass.getInstance()
				.getCollection(collectionName);
		
		
		BasicDBObject query1 = new BasicDBObject();
		query1.append(SuperTop.SOURCE_DISTRIBUTION_ID, dataset1);
		
		BasicDBObject query2 = new BasicDBObject();
		query2.append(SuperTop.TARGET_DISTRIBUTION_ID, dataset2);
		
		BasicDBList and = new BasicDBList();
		and.add(query1);
		and.add(query2);
		
		DBCursor cursor = collection.find(new BasicDBObject("$and",and)).sort(new BasicDBObject(SuperTop.AMOUNT, -1));
//		System.out.println(new BasicDBObject("$and",and));
		
		for(DBObject object: cursor){
			result.put(object.get(SuperTop.LINK).toString(), Integer.parseInt(object.get(SuperTop.AMOUNT).toString()));
		}
		
		return result;

	}

}
