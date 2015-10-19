package lodVader.mongodb.collections;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.DBSuperClass;

public class ResourceDB extends DBSuperClass {
	
	public final String MODIFIED_TIMESTAMP = "modifiedTimestamp";

	public static final String LOD_VADER_ID = "lodVaderID";
	
	public static final String IS_VOCABULARY = "isVocabulary";
	
	public static final String TITLE = "title";
	
	public static final String LABEL = "label";
	
	
	
	protected String title;
	
	protected String label;
	
	protected boolean isVocabulary = false;
	
	protected int lodVaderID = 0;
	

	protected DBObject search(int id) {

		mongoDBObject.put(LOD_VADER_ID,  id);
		DBCursor d = objectCollection.find(mongoDBObject);
		if (d.hasNext()){
			DBObject o = d.next();
			this.uri = o.get(URI).toString();
			return o;
		}

		return null;
	}
	

	public ResourceDB(String collectionName, String uri) {
		super(collectionName, uri);
		// TODO Auto-generated constructor stub
	}
	
	public ResourceDB(String collectionName, int id) {
		super(collectionName, id);
		// TODO Auto-generated constructor stub
	}
	
	public ResourceDB(String collectionName, DBObject object) {
		super(collectionName, object);
		// TODO Auto-generated constructor stub
	}
	
	public ResourceDB(String collection, String uri, boolean isRegex) {
		super(collection, uri, isRegex);
		loadObject();
	}

	@Override
	public boolean updateObject(boolean checkBeforeInsert)
			throws LODVaderLODGeneralException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean loadObject() {
		return false;
	}

}
