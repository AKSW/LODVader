package lodVader.mongodb.collections.RDFResources.rdfType;

import java.util.Iterator;
import java.util.Set;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceDB;

public class RDFTypeObjectDB extends GeneralRDFResourceDB {

	public static final String COLLECTION_NAME = "RDFTypeObjects";

	public RDFTypeObjectDB(String uri) {
		super(COLLECTION_NAME, uri);
	}

	public RDFTypeObjectDB(int lodVaderID) {
		super(COLLECTION_NAME, lodVaderID);
	}

	public RDFTypeObjectDB() {
		super(COLLECTION_NAME);
	}

	@Override
	public void insertSet(Set<String> set) {
		Iterator<String> i = set.iterator();
		RDFTypeObjectDB t = null;
		while (i.hasNext()) {
			t = new RDFTypeObjectDB(i.next());
			try {
				t.update(true);
			} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException
					| LODVaderNoPKFoundException e) {
				e.printStackTrace();
			}
		}
	}
}
