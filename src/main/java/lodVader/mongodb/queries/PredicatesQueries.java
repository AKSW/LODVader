package lodVader.mongodb.queries;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;

public class PredicatesQueries {

	/**
	 * Return a set of predicates of distribution
	 * @param distributionID
	 * @return set of string
	 */
	public HashSet<String> getSetOfPredicates(
			int distributionID) {
		
		HashSet<String>  result= new HashSet<String>();
		try {

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					AllPredicatesRelationDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(
					AllPredicatesRelationDB.DISTRIBUTION_ID,
					distributionID);

			DBCursor cursor = collection.find(query);

			while (cursor.hasNext()) {
				result.add(((Number) cursor.next().get(
						AllPredicatesRelationDB.PREDICATE_ID)).toString());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * insert a set of predicates of a distribution
	 * @param predicates set of predicates
	 * @param distributionLODVaderID dynamiclod id of distribution that contains the predicates
	 * @param topDatasetDynLodID dynamiclod id of top dataset of the distribution that contains the predicates
	 */
	public void insertPredicates(Set<String> predicates, int distributionLODVaderID, int topDatasetDynLodID){
		// save predicates
		Iterator<String> i = predicates.iterator();
		while(i.hasNext()){
			String predicate = i.next();
			AllPredicatesDB p = new AllPredicatesDB(predicate);
			try {
				p.updateObject(true);
				AllPredicatesRelationDB pr = new AllPredicatesRelationDB(p.getLodVaderID()+"-"+distributionLODVaderID+"-"+topDatasetDynLodID);
				pr.setDatasetID(topDatasetDynLodID);
				pr.setDistributionID(distributionLODVaderID);
				pr.setPredicateID(p.getLodVaderID());
				pr.updateObject(true);
			} catch (LODVaderLODGeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}
