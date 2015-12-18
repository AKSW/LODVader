package lodVader.mongodb.collections.RDFResources;

import java.util.Set;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.LODVaderCounterDB;

public abstract class GeneralRDFResourceDB extends DBSuperClass2  {

	public static final String LOD_VADER_ID = "lodVaderID";

	public static final String URI = "uri";
	
	
	public GeneralRDFResourceDB(String collection, int lodVaderID) {
		super(collection);
		setVariables();
		setLODVaderID(lodVaderID);
		find(true);
	}
	
	public GeneralRDFResourceDB(String collection) {
		super(collection);
		setVariables();
	}
	
	public GeneralRDFResourceDB(String collection, String uri) {
		super(collection);
		setVariables();
		setUri(uri);
		if(!find(true))
			setLODVaderID(new LODVaderCounterDB().incrementAndGetID());
		
	}
	
	public void setVariables(){
		addPK(LOD_VADER_ID);
		addPK(URI);
		addMandatoryField(LOD_VADER_ID);
		addMandatoryField(URI);
	}


	public int getLodVaderID() {
		return Integer.parseInt(getField(LOD_VADER_ID).toString()); 
	}
	
	public String getUri() {
		return getField(URI).toString();
	}

	public void setUri(String uri) {
		addField(URI, uri);
	}	
	
	public void setLODVaderID(int lodVaderID) {
		addField(LOD_VADER_ID, lodVaderID);
	}

	
	abstract public void insertSet(Set<String> set);	
	
}
