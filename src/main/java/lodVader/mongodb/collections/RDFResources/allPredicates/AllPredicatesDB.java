package lodVader.mongodb.collections.RDFResources.allPredicates;

import java.util.Iterator;
import java.util.Set;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceDB;



public class AllPredicatesDB extends GeneralRDFResourceDB {

	public static final String COLLECTION_NAME = "allPredicates";

	public AllPredicatesDB(int id) {
		super(COLLECTION_NAME, id);
		loadObject();
	}

	public AllPredicatesDB(String URI) {
		super(COLLECTION_NAME, URI);
		loadObject();
	}
	
	public AllPredicatesDB() {
		super();
	}

	@Override
	public void loadLocalVariables() {
	}

	@Override
	public void updateLocalVariables() {
	}

	@Override
	public void insertSet(Set<String> set) {
		Iterator<String> i = set.iterator();
		AllPredicatesDB t = null;
		while(i.hasNext()){
			t=new AllPredicatesDB(i.next());
			try {
				t.updateObject(true);
			} catch (LODVaderLODGeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
