package lodVader.mongodb.collections.RDFResources.owlClass;

import java.util.Iterator;
import java.util.Set;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceDB;

public class OwlClassDB extends GeneralRDFResourceDB{

	public static final String COLLECTION_NAME = "OWLClasses";

	public OwlClassDB() {
		super(COLLECTION_NAME); 
	}
	
	public OwlClassDB(int lodVaderId) {
		super(COLLECTION_NAME, lodVaderId);
	}
	
	public OwlClassDB(String url) {
		super(COLLECTION_NAME,url);
	}


	@Override
	public void insertSet(Set<String> set) {
		Iterator<String> i = set.iterator();
		OwlClassDB t = null;
		while(i.hasNext()){ 
			t=new OwlClassDB(i.next()); 
			try {
				t.update(true);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
