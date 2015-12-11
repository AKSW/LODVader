package lodVader.mongodb.collections.RDFResources.rdfSubClassOf;

import java.util.HashMap;
import java.util.HashSet;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;

public class RDFSubClassOfRelationDB extends GeneralRDFResourceRelationDB{

	public static final String COLLECTION_NAME = "RDFSubClassOfRelation";

	public RDFSubClassOfRelationDB(String id) {
		super(COLLECTION_NAME, id);
	}
	
	public RDFSubClassOfRelationDB() {
		super(COLLECTION_NAME);
	}

	/**
	 * Store a set of object rdf:type values
	 * @param set
	 */
	public void insertSet(HashMap<String, Integer> set, int distributionLODVaderID, int topDatasetLODVaderID){
		for(String object : set.keySet()){
			RDFSubClassOfDB p = new RDFSubClassOfDB(object);
			try {
				p.update(true);
				RDFSubClassOfRelationDB pr = new RDFSubClassOfRelationDB(p.getLodVaderID()+"-"+distributionLODVaderID+"-"+topDatasetLODVaderID);
				pr.setDatasetID(topDatasetLODVaderID);
				pr.setDistributionID(distributionLODVaderID);
				pr.setPredicateID(p.getLodVaderID());
				pr.setAmount(set.get(object));
				pr.update(true);
			} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException | LODVaderNoPKFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Return a set of predicates of distribution
	 * @param distributionID
	 * @return set of string
	 */
	@Override
	public HashSet<String> getSetOfPredicates(
			int distributionID) {
		
		HashSet<String> result = new HashSet<String>();
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(
					RDFSubClassOfRelationDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(
					RDFSubClassOfRelationDB.DISTRIBUTION_ID,
					distributionID);

			DBCursor cursor = collection.find(query);
			while (cursor.hasNext()) {
				result.add(((Number) cursor.next().get(
						RDFSubClassOfRelationDB.PREDICATE_ID)).toString());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
