package lodVader.threads;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.RDFHandlerBase;

import lodVader.LODVaderProperties;

public class SplitAndStoreThread extends RDFHandlerBase {

	final static Logger logger = Logger.getLogger(SplitAndStoreThread.class);

	private String fileName;

	private boolean doneReadingFile = false;

	ConcurrentLinkedQueue<String> objectQueue = null;

	ConcurrentLinkedQueue<String> subjectQueue = null;

	public Integer subjectLines = 0;

	public Integer objectLines = 0;
	
	public Integer totalTriplesRead = 0;
	
	private String lastSubject = "";

	public boolean isChain = true;

	private int bufferSize = 100000;

	// tmp files to store subjects and objects. 
	// these files will be used to create Bloom folters
	BufferedWriter subjectFile = null;
	BufferedWriter objectFile = null;
	
	// saving all predicates
	public  HashMap<String, Integer> allPredicates = new  HashMap<String, Integer>();

	// saving all rdf type
	public HashMap<String, Integer> rdfTypeSubjects = new HashMap<String, Integer>();
	public HashMap<String, Integer> rdfTypeObjects = new HashMap<String, Integer>();

	public HashMap<String, Integer> owlClasses = new  HashMap<String, Integer>();
	public HashMap<String, Integer> rdfSubClassOf= new HashMap<String, Integer>();
	
	
	public SplitAndStoreThread(ConcurrentLinkedQueue<String> subjectQueue,
			ConcurrentLinkedQueue<String> objectQueue, String fileName) {
		this.objectQueue = objectQueue;
		this.subjectQueue = subjectQueue;
		this.fileName = fileName;
		startQueues();

	}


	private void startQueues() {
		try {
			if (subjectQueue != null)
				subjectFile = new BufferedWriter(new FileWriter(
						LODVaderProperties.SUBJECT_FILE_DISTRIBUTION_PATH
								+ fileName));
			if (objectQueue != null)
				objectFile = new BufferedWriter(new FileWriter(
						LODVaderProperties.OBJECT_FILE_DISTRIBUTION_PATH
								+ fileName));					
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void closeQueues() {
		try {
			if (objectFile != null)
				objectFile.close();
			if (subjectFile != null)
				subjectFile.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
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

	private void addToMap(HashMap<String, Integer> map, String value){
		int n=0;
		if(map.get(value)!=null)
			n=map.get(value);
		map.put(value, n+1);
	}
	
	public void saveStatement(String stSubject, String stPredicate,
			String stObject) {		
		
		
		if(stObject.startsWith("<")){
//			System.out.println(stObject);
			stObject = stObject.substring(1, stObject.length() -1);
//			System.out.println(stObject);

		}
		
//		http://www.w3.org/1999/02/22-rdf-syntax-ns#
//			if(stPredicate.equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#>")){
		// save rdf:type
		
		addToMap(allPredicates, stPredicate);

		if(stObject.equals("http://www.w3.org/2002/07/owl#Class")){
			addToMap(owlClasses, stSubject);
		}
		else if(stPredicate.equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")){
			addToMap(rdfSubClassOf, stObject);
		}
		else if(stPredicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")){
//			addToMap(rdfTypeSubjects, triple.getSubject().toString());
			addToMap(rdfTypeObjects, stObject);
		}
		
//		if(stPredicate.equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")){
//			addToMap(rdfTypeSubjects, stSubject);
//			addToMap(rdfTypeObjects, stObject);
//		}
		
		// save predicate
		
		try {
//			if (true) {

//				if (!stObject.equals("http://www.w3.org/2002/07/owl#Class")
//						&& !stPredicate
//								.equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {

				
				// compare the current subject with the previous one
				if(!stSubject.equals(lastSubject)){
					
					// get subject and save to file
					subjectFile.write(stSubject+"\n");
					subjectLines++;
					lastSubject = stSubject;
					if (isChain)
						subjectQueue.add(stSubject);
					
				}

				// get object (make sure that its a resource and not a literal), add
				// to queue and save to file
				if (!stObject.startsWith("\"")) {
					objectFile.write(stObject+"\n");

					// add object to object queue (the queue is read by another thread)
					if (isChain)
						objectQueue.add(stObject);
					objectLines++;

//					System.out.println(stSubject+ " "+ stPredicate+" "+stObject);
				}
//			}
			while (objectQueue.size() > bufferSize) {
				Thread.sleep(1);
			}
			while (subjectQueue.size() > bufferSize) {
				Thread.sleep(1);
				System.out.println(subjectQueue.size());
			}
			
			if (totalTriplesRead % 1000000 == 0) {
				logger.info("Triples read: " + totalTriplesRead);
				// System.out.println(objectQueue.size());
				// System.out.println(subjectQueue.size());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		totalTriplesRead++;


	}

	@Override
	public void handleStatement(Statement st) {
		String stSubject = st.getSubject().toString();
		String stPredicate = st.getPredicate().toString();
		String stObject = st.getObject().toString();
		saveStatement(stSubject, stPredicate, stObject);
	}
	
}
