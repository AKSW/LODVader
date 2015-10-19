package lodVader.mongodb;

import java.util.Arrays;
import java.util.Date;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import lodVader.LODVaderProperties;
import lodVader.exceptions.LODVaderLODGeneralException;

abstract public class DBSuperClass {

	// defining mongodb connection
	protected static MongoClient mongo = null;

	// defining mongodb database
	static DB db;

	// defining collection name
	protected String collectionName;

	protected DBCollection objectCollection;

	protected BasicDBObject mongoDBObject = new BasicDBObject();

	// defining mongodb keys -> RDF properties

	public static final String URI = "_id";

	protected String uri = null;
	
	protected boolean isRegex = false;

	// abstract methods
	abstract public boolean updateObject(boolean checkBeforeInsert)
			throws LODVaderLODGeneralException;

	abstract protected boolean loadObject();

	public DBSuperClass(){
		
	}
	/**
	 * @param collectionName
	 * The collection name
	 * @param uri
	 * The resource URI
	 */
	public DBSuperClass(String collectionName, String uri) {

		try {
			getInstance();

			this.collectionName = collectionName;

			objectCollection = getInstance().getCollection(collectionName);

			mongoDBObject.put(URI, uri);

			this.uri = uri;

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public DBSuperClass(String collectionName, String uri, boolean isRegex) {

		try {
			getInstance();

			this.collectionName = collectionName;

			objectCollection = getInstance().getCollection(collectionName);

			if(isRegex)
				mongoDBObject.put(URI,  new BasicDBObject("$regex", uri+".*"));

			this.uri = uri;

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public DBSuperClass(String collectionName, DBObject object) {
		
		try {
			getInstance();

			this.collectionName = collectionName;

			objectCollection = getInstance().getCollection(collectionName);

			mongoDBObject.put(URI,  object.get(URI).toString());

			this.uri = object.get(URI).toString();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public DBSuperClass(String collectionName, int id) {

		try {
			getInstance();

			this.collectionName = collectionName;
			objectCollection = getInstance().getCollection(collectionName);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	protected DBObject searchByID(int id){
		return null;
	}
	

	public static DB getInstance() {
		try {
			if (mongo == null) {
				LODVaderProperties p = new LODVaderProperties();
				p.loadProperties();
				if (LODVaderProperties.MONGODB_SECURE_MODE) {
					MongoCredential credential = MongoCredential
							.createMongoCRCredential(
									LODVaderProperties.MONGODB_USERNAME,
									LODVaderProperties.MONGODB_DB,
									LODVaderProperties.MONGODB_PASSWORD
											.toCharArray());
					mongo = new MongoClient(new ServerAddress(
							LODVaderProperties.MONGODB_HOST),
							Arrays.asList(credential));
				} else {
					mongo = new MongoClient(
							LODVaderProperties.MONGODB_HOST,
							LODVaderProperties.MONGODB_PORT);
				}
				db = mongo.getDB(LODVaderProperties.MONGODB_DB);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return db;
	}

	protected void insert(boolean checkBeforeInsert) throws LODVaderLODGeneralException {

		// adding object URI
		if (uri == null){
			throw new LODVaderLODGeneralException(
					"Error while saving. Object URI can't be null.");
		}
		// check if URI already exists
		if (checkBeforeInsert) {
			BasicDBObject tmp = new BasicDBObject();
			tmp.put(URI, uri);
			DBCursor d = objectCollection.find(tmp);
			if (d.hasNext())
				throw new LODVaderLODGeneralException("Can't save object with URI: " + uri
						+ ". Object already exists.");
		}


		// saving object to mongodb
		objectCollection.insert(mongoDBObject);
	}

	protected boolean update() throws LODVaderLODGeneralException {

		if (uri == null)
			return false;

		BasicDBObject query = new BasicDBObject();
		query.put(URI, uri);
		BasicDBObject objectToUpdate = (BasicDBObject) mongoDBObject.clone();
		objectToUpdate.removeField("_id");

		BasicDBObject updateObj = new BasicDBObject();
		updateObj.put("$set", objectToUpdate);

		if (objectCollection.update(query, updateObj).isUpdateOfExisting())
			return true;
		else {
			throw new LODVaderLODGeneralException("Object with URI: " + uri
					+ " could not be found in database.");
		}
	}
	
	public boolean remove(){
		DBCursor d = objectCollection.find(mongoDBObject); 
		if(d.hasNext())
			objectCollection.remove(d.next());
		return true;
	}

	protected DBObject search() {

		// adding object URI
//		if (uri == null)
//			return null;

		DBCursor d = objectCollection.find(mongoDBObject);
		if (d.hasNext())
			return d.next();

		return null;
	}

	public String getUri() {
		return uri;
	}
}
