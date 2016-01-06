package lodVader.mongodb.queries;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;

public class PredicatesQueries {

	/**
	 * Return a set of predicates of distribution
	 * 
	 * @param distributionID
	 * @return set of string
	 */
	public HashSet<String> getSetOfPredicates(int distributionID) {

		HashSet<String> result = new HashSet<String>();
		try {

			DBCollection collection = DBSuperClass2.getDBInstance()
					.getCollection(AllPredicatesRelationDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(AllPredicatesRelationDB.DISTRIBUTION_ID, distributionID);

			DBCursor cursor = collection.find(query);

			while (cursor.hasNext()) {
				result.add(((Number) cursor.next().get(AllPredicatesRelationDB.PREDICATE_ID)).toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * insert a set of predicates of a distribution
	 * 
	 * @param predicates
	 *            set of predicates
	 * @param distributionLODVaderID
	 *            dynamiclod id of distribution that contains the predicates
	 * @param topDatasetDynLodID
	 *            dynamiclod id of top dataset of the distribution that contains
	 *            the predicates
	 */
	public void insertPredicates(Set<String> predicates, int distributionLODVaderID, int topDatasetDynLodID) {
		// save predicates
		Iterator<String> i = predicates.iterator();
		while (i.hasNext()) {
			String predicate = i.next();
			AllPredicatesDB p = new AllPredicatesDB(predicate);
			try {
				p.update(true);
				AllPredicatesRelationDB pr = new AllPredicatesRelationDB(
						p.getLodVaderID() + "-" + distributionLODVaderID + "-" + topDatasetDynLodID);
				pr.setDatasetID(topDatasetDynLodID);
				pr.setDistributionID(distributionLODVaderID);
				pr.setPredicateID(p.getLodVaderID());
				pr.update(true);
			} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException
					| LODVaderNoPKFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public HashSet<AllPredicatesDB> getAllPredicatesRegex(String predicate) {
		HashSet<AllPredicatesDB> result = new HashSet<AllPredicatesDB>();

		DBCollection collection = DBSuperClass2.getDBInstance().getCollection(AllPredicatesDB.COLLECTION_NAME);

		DBObject query = new BasicDBObject(AllPredicatesDB.URI, new BasicDBObject("$regex", predicate + ".*"));

		DBCursor d = collection.find(query);
		while (d.hasNext()) {
			result.add(new AllPredicatesDB(d.next()));
		}

		return result;
	}

	public HashMap<Integer, HashMap<String, Integer>> getDistributions(Set<AllPredicatesDB> predicates) {

		DBCollection collection = DBSuperClass2.getDBInstance().getCollection(AllPredicatesRelationDB.COLLECTION_NAME);

		HashMap<Integer, HashMap<String, Integer>> result = new HashMap<Integer, HashMap<String, Integer>>();

		HashMap<Integer, String> predicatesMap = new HashMap<Integer, String>();

		DBObject query;
		
		if (predicates != null) {
			for (AllPredicatesDB predicate : predicates) {
				predicatesMap.put(predicate.getLodVaderID(), predicate.getUri());
			}
			query = new BasicDBObject(AllPredicatesRelationDB.PREDICATE_ID,
					new BasicDBObject("$in", predicatesMap.keySet()));
		} else {
			DBCollection collection2 = DBSuperClass2.getDBInstance().getCollection(AllPredicatesDB.COLLECTION_NAME);
			DBCursor cursor = collection2.find();
			while (cursor.hasNext()) {
				AllPredicatesDB tmp = new AllPredicatesDB(cursor.next());
				predicatesMap.put(tmp.getLodVaderID(), tmp.getUri());
			}
			
			query = new BasicDBObject();
		}

		DBCursor d = collection.find(query);
		AllPredicatesRelationDB tmp;

		while (d.hasNext()) {
			tmp = new AllPredicatesRelationDB(d.next());

			if (result.get(tmp.getDistributionID()) == null) {
				HashMap<String, Integer> str = new HashMap<String, Integer>();
				str.put(predicatesMap.get(tmp.getPredicateID()), tmp.getAmount());
				result.put(tmp.getDistributionID(), str);
			} else {
				HashMap<String, Integer> str = result.get(tmp.getDistributionID());
				str.put(predicatesMap.get(tmp.getPredicateID()), tmp.getAmount());
				result.put(tmp.getDistributionID(), str);
			}
		}
	
		return result;
	}

}
