package lodVader.mongodb.collections.RDFResources.owlClass;

import java.util.Iterator;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;

import lodVader.configuration.Config;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceDB;

public class OwlClassDB extends GeneralRDFResourceDB{
	
	@Autowired
	Config conf;

	public static final String COLLECTION_NAME = "OWLClasses";

	public OwlClassDB(DBSuperClass2 db) {
		super(db); 
		super.init(COLLECTION_NAME);
	}	
	
	public  void init(int lodVaderId) {
		super.init(COLLECTION_NAME, lodVaderId);
	}
	
	public  void init(String url) {
		super.init(COLLECTION_NAME, url);
	}


	@Override
	public void insertSet(Set<String> set) {
		Iterator<String> i = set.iterator();
		OwlClassDB t = null;
		while(i.hasNext()){ 
			t= conf.getOwlClassDB();
			t.init(i.next());
			try {
				t.db.update(true);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
