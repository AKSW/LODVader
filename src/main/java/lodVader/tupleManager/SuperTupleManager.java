package lodVader.tupleManager;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.openrdf.rio.helpers.RDFHandlerBase;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.utils.Timer;

public abstract class SuperTupleManager extends RDFHandlerBase {
		
	protected String fileName;

	protected boolean doneReadingFile = false;

	ConcurrentLinkedQueue<String> objectQueue = null;

	ConcurrentLinkedQueue<String> subjectQueue = null;

	public Integer subjectLines = 0;

	public Integer objectLines = 0;

	public Integer totalTriplesRead = 0;

	protected int bufferSize = 100000;

	protected Timer t = new Timer();

	DistributionDB distribution ;
	
	// saving all predicates
	public HashMap<String, Integer> allPredicates = new HashMap<String, Integer>();

	// saving all rdf type
	public HashMap<String, Integer> rdfTypeObjects = new HashMap<String, Integer>();
	public HashMap<String, Integer> owlClasses = new HashMap<String, Integer>();
	public HashMap<String, Integer> rdfSubClassOf = new HashMap<String, Integer>();
	
	
	public abstract void startFiles();
	public abstract void closeFiles();
	public abstract void saveStatement(String stSubject, String stPredicate, String stObject);
	
	
	protected void addToMap(HashMap<String, Integer> map, String value) {
		int n = 0;
		if (map.get(value) != null)
			n = map.get(value);
		map.put(value, n + 1);
	}
	
	public String getFileName() {
		return fileName;
	}

	public void setDoneReadingFile(boolean doneReadingFile) {
		this.doneReadingFile = doneReadingFile;
	}

	public boolean isDoneReadingFile() {
		return doneReadingFile; 
	}

	public Integer getSubjectLines() {
		return subjectLines;
	}

	public Integer getObjectLines() {
		return objectLines;
	}

	public Integer getTotalTriples() {
		return totalTriplesRead;
	}

}
