package lodVader.mongodb.queries;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.ResourceDB;

public class GeneralQueries {

	public  ArrayList<String> getMongoDBObject(String collectionName,
			String field, String value) {

		ArrayList<String> list = new ArrayList<String>();
		try {
			DBCollection collection = DBSuperClass2.getCollection(
					collectionName);
			DBObject query = new BasicDBObject(field, value);
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(instance.get(ResourceDB.URI)
						.toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;

	}
}

