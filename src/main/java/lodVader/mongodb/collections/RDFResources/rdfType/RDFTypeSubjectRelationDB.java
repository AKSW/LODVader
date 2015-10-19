package lodVader.mongodb.collections.RDFResources.rdfType;

import java.util.HashMap;
import java.util.HashSet;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;



public class RDFTypeSubjectRelationDB extends GeneralRDFResourceRelationDB{

	public static final String COLLECTION_NAME = "RDFTypeSujectsRelation";


	public RDFTypeSubjectRelationDB(int id) {
		super(COLLECTION_NAME, id);
		loadObject();
	}

	public RDFTypeSubjectRelationDB() {
		super();
	}

	public RDFTypeSubjectRelationDB(String URI) {
		super(COLLECTION_NAME, URI);
		loadObject();
	}

	@Override
	public void loadLocalVariables() {
	}

	@Override
	public void updateLocalVariables() {		
	}
	
	/**
	 * Store a set of subjects rdf:type values
	 * @param set
	 */
	public void insertSet(HashMap<String, Integer> set, int distributionLODVaderID, int topDatasetLODVaderID){
		for(String object : set.keySet()){
			RDFTypeSubjectDB p = new RDFTypeSubjectDB(object);
			try {
				p.updateObject(true);
				RDFTypeSubjectRelationDB pr = new RDFTypeSubjectRelationDB(p.getLodVaderID()+"-"+distributionLODVaderID+"-"+topDatasetLODVaderID);
				pr.setDatasetID(topDatasetLODVaderID);
				pr.setDistributionID(distributionLODVaderID);
				pr.setPredicateID(p.getLodVaderID());
				pr.setAmount(set.get(object));
				pr.updateObject(true);
			} catch (LODVaderLODGeneralException e) {
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
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					RDFTypeSubjectRelationDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(
					RDFTypeSubjectRelationDB.DISTRIBUTION_ID,
					distributionID);

			DBCursor cursor = collection.find(query);
			while (cursor.hasNext()) {
				result.add(((Number) cursor.next().get(
						RDFTypeSubjectRelationDB.PREDICATE_ID)).toString());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

}
