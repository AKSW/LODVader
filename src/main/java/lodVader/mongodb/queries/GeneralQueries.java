package lodVader.mongodb.queries;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;

public class GeneralQueries {

	public  ArrayList<String> getMongoDBObject(String collectionName,
			String field, String value) {

		ArrayList<String> list = new ArrayList<String>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					collectionName);
			DBObject query = new BasicDBObject(field, value);
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(instance.get(DBSuperClass.URI)
						.toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;

	}
}

