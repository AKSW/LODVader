package lodVader.mongodb.collections;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;

public class SystemPropertiesDB extends DBSuperClass2 {

	// Collection name
	public static final String COLLECTION_NAME = "SystemProperties";

	public static final String KEY = "key";

	public static final String VALUE = "value";

	public SystemPropertiesDB() {
		super(COLLECTION_NAME);
		addPK(KEY);
	}

	private void setKey(String key) {
		addField(KEY, key);
	}

	private void setValue(Boolean value) {
		addField(VALUE, value);
	}

	public Boolean getDownloadedLOV() {
		setKey("DownloadedLOV");
		find(true);
		if (getField("value") == null)
			return false;
		return Boolean.parseBoolean(getField("value").toString());
	}

	public void setDownloadedLOV(Boolean downloadedLOV) {
		setKey("DownloadedLOV");
		setValue(downloadedLOV);
		try {
			update(true);
		} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException
				| LODVaderNoPKFoundException e) {
			e.printStackTrace();
		}
	}

}
