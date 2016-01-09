package lodVader.mongodb;

import java.util.ArrayList;
import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import lodVader.LODVaderProperties;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;

public class DBSuperClass2 {

	// defining mongodb connection
	private static MongoClient mongo = null;

	// defining mongodb database
	private static DB db;

	// defining collection name
	@JsonIgnore
	public String COLLECTION_NAME = null;

	// defining mongodb _id
	@JsonIgnore
	public String ID = "_id";

	// defining PrimaryKeys field
	@JsonIgnore
	public ArrayList<Object> primaryKey = new ArrayList<Object>();

	// mongodb collection object
	protected DBCollection collection;

	// mongodb object the will persist with this class
	protected DBObject mongoDBObject = new BasicDBObject();

	// list of mandatory fields to check before store the object
	protected ArrayList<String> mandatoryFields = new ArrayList<String>();

	protected void addMandatoryField(String field) {
		mandatoryFields.add(field);
	}

	public DBSuperClass2(String collectionName) {
		this.COLLECTION_NAME = collectionName;
	}

	public DBSuperClass2(String collectionName, DBObject obj) {
		this.COLLECTION_NAME = collectionName;
		this.mongoDBObject = obj;
	}

	// public DBSuperClass2() {
	// }

	// add pair key/value to the persistence object
	protected void addField(String key, String val) {
		mongoDBObject.put(key, val);
	}

	protected void addField(String key, Double val) {
		mongoDBObject.put(key, val);
	}

	// add pair key/value to the persistence object
	protected void addField(String key, int val) {
		mongoDBObject.put(key, val);
	}

	// add pair key/value to the persistence object
	protected void addField(String key, boolean val) {
		mongoDBObject.put(key, val);
	}

	// get a value given a key
	protected Object getField(String key) {
		// if (mongoDBObject.get(key) != null)
		return mongoDBObject.get(key);
		// else
		// return new Object();
	}

	// get a value given a key
	protected Object getField(Integer key) {
		return mongoDBObject.get(String.valueOf(key));
	}

	// add pair key/value to the persistence object
	protected void addField(String key, ArrayList<Integer> val) {
		mongoDBObject.put(key, val);
	}

	static public DBCollection getCollection(String collection) {
		return getDBInstance().getCollection(collection);
	}

	// get mongobd db instance
	public static DB getDBInstance() {
		try {
			if (mongo == null) {
				if (LODVaderProperties.MONGODB_DB == null)
					new LODVaderProperties().loadProperties();
				if (LODVaderProperties.MONGODB_SECURE_MODE) {
					MongoCredential credential = MongoCredential.createMongoCRCredential(
							LODVaderProperties.MONGODB_USERNAME, LODVaderProperties.MONGODB_DB,
							LODVaderProperties.MONGODB_PASSWORD.toCharArray());
					mongo = new MongoClient(new ServerAddress(LODVaderProperties.MONGODB_HOST),
							Arrays.asList(credential));
				} else {
					mongo = new MongoClient(LODVaderProperties.MONGODB_HOST, LODVaderProperties.MONGODB_PORT);
				}
				db = mongo.getDB(LODVaderProperties.MONGODB_DB);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return db;
	}

	/**
	 * Insert a new object in MongoDB database
	 * 
	 * @param checkBeforeInsert
	 *            query database and only insert if object is not there.
	 * @throws LODVaderObjectAlreadyExistsException
	 * @throws LODVaderNoPKFoundException
	 */
	public void insert(boolean checkBeforeInsert)
			throws LODVaderObjectAlreadyExistsException, LODVaderNoPKFoundException {

		if (checkBeforeInsert)
			if (find(false))
				throw new LODVaderObjectAlreadyExistsException(
						"Can't save object with PK: " + getPK() + ". Object already exists.");

		// saving object to mongodb
		getCollection().insert(mongoDBObject);
	}

	/**
	 * Query a object based on the list of PKs.
	 * 
	 * @param update
	 *            update object case found.
	 * @return true case the object is found.
	 */
	public boolean find(boolean update) {

		BasicDBList list = new BasicDBList();

		for (Object pk : getPK()) {
			if (pk instanceof String) {
				String pks = (String) pk;
				if (getField(pks) != null) {
					list.add(new BasicDBObject(pks, getField(pks)));
				}
			} else if (pk instanceof Integer) {
				Integer pks = (Integer) pk;
				if (getField(pks) != null) {
					list.add(new BasicDBObject(String.valueOf(pks), getField(pks)));
				}
			}
		}

		if (list.size() == 0)
			return false;

		DBCursor cursor = getCollection().find(new BasicDBObject("$or", list));

		if (cursor.size() > 0) {
			if (update) {
				mongoDBObject = cursor.next();
			}
			return true;
		} else
			return false;
	}

	/**
	 * Query a object based on the a key
	 * 
	 * @param update
	 *            update object case found.
	 * @return true case the object is found.
	 */
	public boolean find(Boolean update, String key, Object value) {

		DBCursor cursor = getCollection().find(new BasicDBObject(key, value));

		if (cursor.size() > 0) {
			if (update) {
				mongoDBObject = cursor.next();
			}
			return true;
		} else
			return false;
	}

	/**
	 * Update a object based on the a key
	 * 
	 * @param create
	 *            create object case not found.
	 * @return true case the object is found.
	 */
	public void update(Boolean create, String key, Object value) {

		try {
			checkMandatoryFields();
			DBCursor cursor = getCollection().find(new BasicDBObject(key, value));

			if (cursor.size() > 0) {
				mongoDBObject = cursor.next();
				getCollection().update(new BasicDBObject(key, value), mongoDBObject);
			} else {
				if (create) {
					getCollection().insert(mongoDBObject);
				}
			}

		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Update an object.
	 * 
	 * @param create
	 *            set whether the object should be created case doesn't exist.
	 * @return true case successfully updated
	 * @throws LODVaderMissingPropertiesException
	 * @throws LODVaderNoPKFoundException
	 * @throws LODVaderObjectAlreadyExistsException
	 */
	public boolean update(boolean create) throws LODVaderMissingPropertiesException,
			LODVaderObjectAlreadyExistsException, LODVaderNoPKFoundException {

		if (create)
			if (!find(false)) {
				insert(false);
				return true;
			}

		checkMandatoryFields();
		BasicDBList list = new BasicDBList();

		for (Object pk : getPK()) {
			if (pk instanceof String) {
				String pks = (String) pk;
				if (getField(pks) != null) {
					list.add(new BasicDBObject(pks, getField(pks)));
				}
			} else if (pk instanceof Integer) {
				Integer pks = (Integer) pk;
				if (getField(pks) != null) {
					list.add(new BasicDBObject(String.valueOf(pks), getField(pks)));
				}
			}
		}

		getCollection().update(new BasicDBObject("$or", list), mongoDBObject);

		return true;
	}

	public boolean updateBasedOnKeys(boolean create) throws LODVaderMissingPropertiesException,
			LODVaderObjectAlreadyExistsException, LODVaderNoPKFoundException {

		checkMandatoryFields();

		if (create)
			if (!find(false))
				insert(false);

		BasicDBList list = new BasicDBList();

		for (Object pk : getPK()) {
			if (pk instanceof String) {
				String pks = (String) pk;
				if (getField(pks) != null) {
					list.add(new BasicDBObject(pks, getField(pks)));
				}
			} else if (pk instanceof Integer) {
				Integer pks = (Integer) pk;
				if (getField(pks) != null) {
					list.add(new BasicDBObject(String.valueOf(pks), getField(pks)));
				}
			}
		}

		getCollection().update(new BasicDBObject("$and", list), mongoDBObject);

		return true;
	}

	/**
	 * Remove an object
	 * 
	 * @return true case successfully removed
	 * @throws LODVaderMissingPropertiesException
	 */
	public boolean remove() throws LODVaderMissingPropertiesException {
		checkMandatoryFields();
		DBCursor d = collection.find(mongoDBObject);
		if (d.hasNext()) {
			collection.remove(d.next());
			return true;
		}
		return false;
	}

	@JsonIgnore
	protected DBCollection getCollection() {
		if (collection == null) {
			collection = getDBInstance().getCollection(COLLECTION_NAME);
		}
		return collection;
	}

	protected DBObject search() {
		DBCursor d = collection.find(mongoDBObject);
		if (d.hasNext())
			return d.next();
		return null;
	}

	@JsonIgnore
	public ArrayList<Object> getPK() {
		return primaryKey;
	}

	protected void addPK(String pK) {
		primaryKey.add(pK);
	}

	private void checkMandatoryFields() throws LODVaderMissingPropertiesException {
		for (String field : mandatoryFields) {
			checkField(field);
		}
	}

	private boolean checkField(String key) throws LODVaderMissingPropertiesException {
		if (mongoDBObject.get(key) == null)
			throw new LODVaderMissingPropertiesException("Missing filed: " + key.toString());
		return true;
	}

}
