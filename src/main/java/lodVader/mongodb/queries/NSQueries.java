package lodVader.mongodb.queries;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;
import lodVader.enumerators.TuplePart;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.namespaces.DistributionObjectNSDB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;
import lodVader.mongodb.collections.namespaces.SuperNS;

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

	public BloomFilterI getNSBloomFilter(TuplePart part) {

		BloomFilterI filter;

		try {

			DBCollection collection;

			if (part.equals(TuplePart.SUBJECT))
				collection = DBSuperClass2.getDBInstance().getCollection(DistributionSubjectNSDB.COLLECTION_NAME);
			else
				collection = DBSuperClass2.getDBInstance().getCollection(DistributionObjectNSDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject();

			DBCursor cursor = collection.find(query);

			filter = BloomFilterFactory.newBloomFilter(); 
					filter.create(cursor.size(), 0.000001);

			while (cursor.hasNext()) {
				filter.add(cursor.next().get(SuperNS.NS).toString());
			}

			return filter;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public BloomFilterI getNSBloomFilter(TuplePart part, ArrayList<Integer> DistributionIDs) {

		BloomFilterI filter;

		try {

			DBCollection collection;

			if (part.equals(TuplePart.SUBJECT))
				collection = DBSuperClass2.getDBInstance().getCollection(DistributionSubjectNSDB.COLLECTION_NAME);
			else
				collection = DBSuperClass2.getDBInstance().getCollection(DistributionObjectNSDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(SuperNS.DISTRIBUTION_ID, new BasicDBObject("$in", DistributionIDs));

			DBCursor cursor = collection.find(query);

			filter = BloomFilterFactory.newBloomFilter(); 
					filter.create(cursor.size(), 0.000001);

			while (cursor.hasNext()) {
				filter.add(cursor.next().get(SuperNS.NS).toString());
			}

			return filter;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public TreeMap<String, HashSet<Integer>> getNSTree(TuplePart part, Integer datasetID) {

		TreeMap<String, HashSet<Integer>> result = new TreeMap<String, HashSet<Integer>>();

		try {

			DBCollection collection;

			if (part.equals(TuplePart.SUBJECT))
				collection = DBSuperClass2.getDBInstance().getCollection(DistributionSubjectNSDB.COLLECTION_NAME);
			else
				collection = DBSuperClass2.getDBInstance().getCollection(DistributionObjectNSDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = null;

			if (datasetID == null)
				query = new BasicDBObject();
			else
				query = new BasicDBObject(SuperNS.DATASET_ID, datasetID);

			DBCursor cursor = collection.find(query);

			HashSet<Integer> set = null;

			DBObject object = null;

			HashSet<Integer> s = null;

			while (cursor.hasNext()) {
				object = cursor.next();
				set = result.get(object.get(SuperNS.NS));
				if (set == null) {
					s = new HashSet<Integer>();
					s.add(Integer.parseInt(object.get(SuperNS.DISTRIBUTION_ID).toString()));
					result.put(object.get(SuperNS.NS).toString(), s);
				} else {
					set.add(Integer.parseInt(object.get(SuperNS.DISTRIBUTION_ID).toString()));
				}
			}

			return result;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public Integer getNumberNS(TuplePart part) {

		Integer result = null;

		try {

			DBCollection collection;

			if (part.equals(TuplePart.SUBJECT))
				collection = DBSuperClass2.getDBInstance().getCollection(DistributionSubjectNSDB.COLLECTION_NAME);
			else
				collection = DBSuperClass2.getDBInstance().getCollection(DistributionObjectNSDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject();

			DBCursor cursor = collection.find(query);

			result = cursor.size();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

}
