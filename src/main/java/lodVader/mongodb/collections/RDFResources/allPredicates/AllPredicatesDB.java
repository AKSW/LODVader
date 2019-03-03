package lodVader.mongodb.collections.RDFResources.allPredicates;

import java.util.Iterator;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;

import com.mongodb.DBObject;

import lodVader.configuration.Config;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceDB;

public class AllPredicatesDB extends GeneralRDFResourceDB {
	
	@Autowired
	Config conf;

	public static final String COLLECTION_NAME = "allPredicates";
	
	public AllPredicatesDB(DBSuperClass2 db){
		super(db);
	}
	public void init(int id) {
		super.init(COLLECTION_NAME, id);
	}

	public void init(String URI) {
		super.init(COLLECTION_NAME, URI);
	}
	
	public void init(DBObject object) {
		super.init(COLLECTION_NAME);
		super.db.mongoDBObject = object;
	}

	@Override
	public void insertSet(Set<String> set) {
		Iterator<String> i = set.iterator();
		AllPredicatesDB t = null;
		while(i.hasNext()){
			t = conf.getAllPredicatesDB();
			t.init(i.next());
			try {
				t.db.update(true);
			} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException | LODVaderNoPKFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
