package lodVader.mongodb.collections.RDFResources.rdfType;

import java.util.Iterator;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;

import lodVader.configuration.Config;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceDB;

public class RDFTypeObjectDB extends GeneralRDFResourceDB {
	
	@Autowired
	Config conf;

	public static final String COLLECTION_NAME = "RDFTypeObjects";
	
	public RDFTypeObjectDB(DBSuperClass2 db){
		super(db);
	}

	public void init(String uri) {
		super.init(COLLECTION_NAME, uri);
	}

	public void init(int lodVaderID) {
		super.init(COLLECTION_NAME, lodVaderID);
	}

	public void init() {
		super.init(COLLECTION_NAME);
	}

	@Override
	public void insertSet(Set<String> set) {
		Iterator<String> i = set.iterator();
		RDFTypeObjectDB t = null;
		while (i.hasNext()) {
			t = conf.getRDFTypeObjectDB();
			t.init(i.next());
			try {
				t.db.update(true);
			} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException
					| LODVaderNoPKFoundException e) {
				e.printStackTrace();
			}
		}
	}
}
