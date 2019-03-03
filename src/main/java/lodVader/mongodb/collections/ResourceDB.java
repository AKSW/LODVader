package lodVader.mongodb.collections;

import lodVader.mongodb.DBSuperClass2;

public class ResourceDB {
	
	public DBSuperClass2 db;

	public static final String URI = "uri";

	public static final String LOD_VADER_ID = "lodVaderID";

	public static final String IS_VOCABULARY = "isVocabulary";

	public static final String TITLE = "title";

	public static final String LABEL = "label";

	public ResourceDB( DBSuperClass2 db, String collectionName) {
		this.db = db;
		this.db.COLLECTION_NAME = collectionName;
		setIsVocabulary(false);
	}

	public void setLodVaderID(int id) {
		db.addField(LOD_VADER_ID, id);
	}

	public void setIsVocabulary(boolean isVocabulary) {
		db.addField(IS_VOCABULARY, isVocabulary);
	}

	public String getTitle() {
		try {
			return db.getField(TITLE).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public boolean getIsVocabulary() {
		return Boolean.getBoolean(db.getField(IS_VOCABULARY).toString());
	}

	public void setTitle(String title) {
		db.addField(TITLE, title);
	}

	public void setLabel(String label) {
		db.addField(LABEL, label);
	}

	public String getLabel() {
		try {
			return db.getField(LABEL).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public Integer getLODVaderID() {
		if (db.getField(LOD_VADER_ID) != null)
			return ((Number) db.getField(LOD_VADER_ID)).intValue();
		else
			return null;
	}

	public String getUri() {
		return db.getField(URI).toString();
	}

	public void setUri(String uri) {
		db.addField(URI, uri);
	}
}
