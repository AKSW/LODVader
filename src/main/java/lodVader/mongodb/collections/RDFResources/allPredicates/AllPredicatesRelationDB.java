package lodVader.mongodb.collections.RDFResources.allPredicates;

import java.util.HashMap;
import java.util.HashSet;

import org.springframework.beans.factory.annotation.Autowired;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.configuration.Config;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;

public class AllPredicatesRelationDB extends GeneralRDFResourceRelationDB{
	
	@Autowired 
	Config conf;
	
	public static final String COLLECTION_NAME = "allPredicatesResource";
	
	public AllPredicatesRelationDB(DBSuperClass2 db) {
		super(db);
	}
	
	public void init(String id) {
		init(COLLECTION_NAME, id);
	}

	public void init() {
		init(COLLECTION_NAME);
	}
	
	public void init(DBObject object) {
		init(COLLECTION_NAME);
		super.db.mongoDBObject = object;
	}
	
	
	/**
	 * Store a set of subjects rdf:type values
	 * @param set
	 */
	public void insertSet(HashMap<String, Integer> set, int distributionLODVaderID, int topDatasetLODVaderID){
		for(String object : set.keySet()){
			AllPredicatesDB p = conf.getAllPredicatesDB();
			p.init(object);
			try {
				p.db.update(true);
				AllPredicatesRelationDB pr = conf.getAllPredicatesRelationDB();
				pr.init(p.getLodVaderID()+"-"+distributionLODVaderID+"-"+topDatasetLODVaderID);
				pr.setDatasetID(topDatasetLODVaderID);
				pr.setDistributionID(distributionLODVaderID); 
				pr.setPredicateID(p.getLodVaderID());
				pr.setAmount(set.get(object));
				pr.db.update(true);
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
			DBCollection collection = db.getCollection(
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
	
}
