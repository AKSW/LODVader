package lodVader.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;


public class DBSuperClass2 {
	
	@Autowired
	MongoDbFactory mongoClient;

	// defining mongodb connection
//	protected static MongoClient mongo = null;

	// defining mongodb database
	private DB db;

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
	public DBObject mongoDBObject = new BasicDBObject();

	// list of mandatory fields to check before store the object
	protected ArrayList<String> mandatoryFields = new ArrayList<String>();

	public void addMandatoryField(String field) {
		mandatoryFields.add(field);
	}


	public DBSuperClass2() {
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
	public void addField(String key, String val) {
		mongoDBObject.put(key, val);
	}

	public void addField(String key, Double val) {
		mongoDBObject.put(key, val);
	}

	// add pair key/value to the persistence object
	public void addField(String key, int val) {
		mongoDBObject.put(key, val);
	}

	// add pair key/value to the persistence object
	public void addField(String key, boolean val) {
		mongoDBObject.put(key, val);
	}

	// get a value given a key
	public Object getField(String key) {
		// if (mongoDBObject.get(key) != null)
		return mongoDBObject.get(key);
		// else
		// return new Object();
	}

	// get a value given a key
	public Object getField(Integer key) {
		return mongoDBObject.get(String.valueOf(key));
	}

	// add pair key/value to the persistence object
	public void addField(String key, ArrayList<Integer> val) {
		mongoDBObject.put(key, val);
	}

    public DBCollection getCollection(String collection) {
		return getDBInstance().getCollection(collection);
	}

	
	public DB getDBInstance() {
	    return mongoClient.getDb();
	}

	/**
	 * Insert a new object in MongoDB database
	 * 
	 * @param checkBeforeInsert query database and only insert if object is not
	 *                          there.
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
	 * @param update update object case found.
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
	 * @param update update object case found.
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
	 * @param create create object case not found.
	 * @return true case the object is found.
	 */
	public void update(Boolean create, String key, Object value) {

		try {
			checkMandatoryFields();
			if (create)
				getCollection().update(new BasicDBObject(key, value), mongoDBObject, true, false);
//				else 
//					DBCursor cursor = getCollection().find(new BasicDBObject(key, value)); 

//				if (cursor.size() > 0) {
//					// mongoDBObject = cursor.next();
//				} else {
//					if (create) {
//						getCollection().insert(mongoDBObject);
//					}
//				}

		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Update an object.
	 * 
	 * @param create set whether the object should be created case doesn't exist.
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

	/**
	 * MongoDB bulk save
	 * 
	 * @return
	 */

	protected boolean bulkSave2(List<DBObject> objects) {
		if (objects.size() == 0)
			return false;
		BulkWriteOperation builder = getCollection().initializeUnorderedBulkOperation();
		for (DBObject doc : objects) {
			builder.insert(doc);
		}
		BulkWriteResult result = builder.execute();
		return result.isAcknowledged();
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

	public void addPK(String pK) {
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
