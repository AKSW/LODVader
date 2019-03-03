package lodVader.mongodb.collections;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;

public class SystemPropertiesDB {
	
	DBSuperClass2 db;

	// Collection name
	public static final String COLLECTION_NAME = "SystemProperties";

	public static final String KEY = "key";

	public static final String VALUE = "value";

	public SystemPropertiesDB(DBSuperClass2 db) {
		this.db = db;
		this.db.COLLECTION_NAME = COLLECTION_NAME;
		this.db.addPK(KEY);
	}

	private void setKey(String key) {
		this.db.addField(KEY, key);
	}

	private void setValue(Boolean value) {
		this.db.addField(VALUE, value);
	}

	public Boolean getDownloadedLOV() {
		setKey("DownloadedLOV");
		this.db.find(true);
		if (this.db.getField("value") == null)
			return false;
		return Boolean.parseBoolean(db.getField("value").toString());
	}

	public void setDownloadedLOV(Boolean downloadedLOV) {
		setKey("DownloadedLOV");
		setValue(downloadedLOV);
		try {
			db.update(true);
		} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException
				| LODVaderNoPKFoundException e) {
			e.printStackTrace();
		}
	}

}
