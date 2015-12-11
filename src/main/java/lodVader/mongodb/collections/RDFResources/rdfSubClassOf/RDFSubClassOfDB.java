package lodVader.mongodb.collections.RDFResources.rdfSubClassOf;

import java.util.Iterator;
import java.util.Set;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceDB;

public class RDFSubClassOfDB extends GeneralRDFResourceDB {

	public static final String COLLECTION_NAME = "RDFSubClassOf";

	public RDFSubClassOfDB(String uri) {
		super(COLLECTION_NAME, uri);
	}	
	public RDFSubClassOfDB(int lodVaderID) {
		super(COLLECTION_NAME, lodVaderID);
		find(true);
	}
	public RDFSubClassOfDB() {
		super(COLLECTION_NAME);
	}

	@Override
	public void insertSet(Set<String> set) {
		Iterator<String> i = set.iterator();
		RDFSubClassOfDB t = null;
		while (i.hasNext()) {
			t = new RDFSubClassOfDB(i.next());
			try {
				t.update(true);
			} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException
					| LODVaderNoPKFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
