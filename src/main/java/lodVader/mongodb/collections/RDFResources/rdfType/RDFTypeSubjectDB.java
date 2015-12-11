package lodVader.mongodb.collections.RDFResources.rdfType;

import java.util.Iterator;
import java.util.Set;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceDB;

public class RDFTypeSubjectDB extends GeneralRDFResourceDB{

	public static final String COLLECTION_NAME = "RDFTypeSubjects";

	public RDFTypeSubjectDB(String id) {
		super(COLLECTION_NAME, id);
	}
	public RDFTypeSubjectDB() {
		super(COLLECTION_NAME);
	}

	@Override
	public void insertSet(Set<String> set) {
		Iterator<String> i = set.iterator();
		RDFTypeSubjectDB t = null;
		while(i.hasNext()){
			t=new RDFTypeSubjectDB(i.next());
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
