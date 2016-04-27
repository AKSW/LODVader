package lodVader.mongodb.collections;

import lodVader.enumerators.TuplePart;
import lodVader.mongodb.DBSuperClass2;

public class LinksAssessment extends DBSuperClass2 {

	public LinksAssessment() {
		super(COLLECTION_NAME);
		addPK(LINK);
		addMandatoryField(ERROR);
	}
	
	public LinksAssessment(String link, String type, TuplePart tuplePart, Integer code, String error) {
		super(COLLECTION_NAME);
		addPK(LINK);
		addMandatoryField(TYPE);
		setLink(link);
		setType(type);
		setError(error);
		setCode(code);
		setTuplePart(tuplePart.toString());
		setError(error);
	}
	
	
	

	// Collection name
	public static final String COLLECTION_NAME = "LinksAssessment";

	public static final String LINK = "link";
	
	public static final String TYPE = "type";
	
	public static final String TUPLE_PART = "tuplePart";
	
	public static final String CODE = "code";
	
	public static final String ERROR = "error";
	
	
	public void setTuplePart(String tuplePart){
		addField(TUPLE_PART, tuplePart);
	}
	
	
	public void setCode(Integer code){
		addField(CODE, code);
	} 
	
	 
	public void setLink(String link){
		addField(LINK, link);
	}
	
	public void setType(String type){
		addField(TYPE, type);
	}
	
	public void setError(String error){
		addField(ERROR, error);
	}

}
