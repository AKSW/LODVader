package lodVader.mongodb.queries;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DistributionObjectNSDB;

public class NSQueries {
	public int getNumberOfObjectResources(
			int distributionID) {
		
		int result=0;
		try {

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionObjectNSDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(
					DistributionObjectNSDB.DISTRIBUTION_ID,
					distributionID);

			DBCursor cursor = collection.find(query);

			while (cursor.hasNext()) {
				result = result + ((Number) cursor.next().get(
						DistributionObjectNSDB.NUMBER_OF_RESOURCES)).intValue();
			}

			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	} 
}
