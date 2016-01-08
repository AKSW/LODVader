package lodVader.mongodb.queries;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.namespaces.DistributionObjectNSDB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;

public class NSQueries {
	public int getNumberOfObjectResources(int distributionID) {

		int result = 0;
		try {

			DBCollection collection = DBSuperClass2.getDBInstance()
					.getCollection(DistributionObjectNSDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(DistributionObjectNSDB.DISTRIBUTION_ID, distributionID);

			DBCursor cursor = collection.find(query);

			while (cursor.hasNext()) {
				result = result + ((Number) cursor.next().get(DistributionObjectNSDB.NUMBER_OF_RESOURCES)).intValue();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	
	public ArrayList<DistributionSubjectNSDB> getSubjectNSByDistribution(int distributionID) {

		ArrayList<DistributionSubjectNSDB> result = new ArrayList<DistributionSubjectNSDB>();
		
		try {

			DBCollection collection = DBSuperClass2.getDBInstance()
					.getCollection(DistributionSubjectNSDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(DistributionSubjectNSDB.DISTRIBUTION_ID, distributionID);

			DBCursor cursor = collection.find(query);

			while (cursor.hasNext()) {
				result.add(new DistributionSubjectNSDB(cursor.next()));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

}
