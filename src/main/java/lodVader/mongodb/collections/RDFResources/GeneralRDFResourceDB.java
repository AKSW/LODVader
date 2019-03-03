package lodVader.mongodb.collections.RDFResources;

import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.LODVaderCounterDB;

public abstract class GeneralRDFResourceDB {
	
	@Autowired
	LODVaderCounterDB lodVaderCounterDB;
	
	@Autowired
	public DBSuperClass2 db;

	public static final String LOD_VADER_ID = "lodVaderID";

	public static final String URI = "uri";
	
	
	public GeneralRDFResourceDB(DBSuperClass2 db) {
		this.db = db;
	}
	
	public void init(String collection, int lodVaderID) {
		db.COLLECTION_NAME = collection;
		setVariables();
		setLODVaderID(lodVaderID);
		db.find(true);
	}
	
	public void init(String collection) {
		db.COLLECTION_NAME = collection;
		setVariables();
	}
	
	public void init(String collection, String uri) {
		db.COLLECTION_NAME = collection;
		setVariables();
		setUri(uri);
		if(!db.find(true))
			setLODVaderID(lodVaderCounterDB.incrementAndGetID());
		
	}
	
	public void setVariables(){
		db.addPK(LOD_VADER_ID);
		db.addPK(URI);
		db.addMandatoryField(LOD_VADER_ID);
		db.addMandatoryField(URI);
	}


	public int getLodVaderID() {
		return Integer.parseInt(db.getField(LOD_VADER_ID).toString()); 
	}
	
	public String getUri() {
		return db.getField(URI).toString();
	}

	public void setUri(String uri) {
		db.addField(URI, uri);
	}	
	
	public void setLODVaderID(int lodVaderID) {
		db.addField(LOD_VADER_ID, lodVaderID);
	}

	
	abstract public void insertSet(Set<String> set);	
	
}
