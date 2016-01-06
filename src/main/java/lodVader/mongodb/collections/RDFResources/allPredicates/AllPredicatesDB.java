package lodVader.mongodb.collections.RDFResources.allPredicates;

import java.util.Iterator;
import java.util.Set;

import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceDB;

public class AllPredicatesDB extends GeneralRDFResourceDB {

	public static final String COLLECTION_NAME = "allPredicates";

	public AllPredicatesDB(int id) {
		super(COLLECTION_NAME, id);
	}

	public AllPredicatesDB(String URI) {
		super(COLLECTION_NAME, URI);
	}
	
	public AllPredicatesDB() {
		super(COLLECTION_NAME);
	}
	
	public AllPredicatesDB(DBObject object) {
		super(COLLECTION_NAME);
		mongoDBObject = object;
	}

	@Override
	public void insertSet(Set<String> set) {
		Iterator<String> i = set.iterator();
		AllPredicatesDB t = null;
		while(i.hasNext()){
			t=new AllPredicatesDB(i.next());
			try {
				t.update(true);
			} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException | LODVaderNoPKFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
