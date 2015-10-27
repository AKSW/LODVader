package lodVader.threads;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.RDFHandlerBase;

import lodVader.LODVaderProperties;
import lodVader.utils.Timer;

public class SplitAndStoreThread extends RDFHandlerBase {

	final static Logger logger = Logger.getLogger(SplitAndStoreThread.class);

	private String fileName;

	private boolean doneReadingFile = false;

	ConcurrentLinkedQueue<String> objectQueue = null;

	ConcurrentLinkedQueue<String> subjectQueue = null;

//	ArrayList<String> subjects = new ArrayList<String>();
//
//	ArrayList<String> objects = new ArrayList<String>();
	
	public Integer subjectLines = 0;

	public Integer objectLines = 0;
	
	public Integer totalTriplesRead = 0;
	
	private String lastSubject = "";

	public boolean isChain = true;

	private int bufferSize = 100000;
	
	private Timer t = new Timer();
	
	int distributionID;
	
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
			ConcurrentLinkedQueue<String> objectQueue, String fileName, int distributionID) {
		this.objectQueue = objectQueue;
		this.subjectQueue = subjectQueue;
		this.fileName = fileName;
		this.distributionID = distributionID; 

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
					
					if(stSubject.startsWith("htt")){
						// get subject and save to file
						subjectFile.write(stSubject+"\n");
//						subjects.add(stSubject);
						
						subjectLines++;
						lastSubject = stSubject;
						if (isChain)
							subjectQueue.add(stSubject);
					}
					
				}

				// get object (make sure that its a resource and not a literal), add
				// to queue and save to file
				if (!stObject.startsWith("\"")) {
					objectFile.write(stObject+"\n");
//					objects.add(stObject);

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
//				System.out.println(subjectQueue.size());
			}
			
//			if(totalTriplesRead % 500000== 0){
			
//				if(objects.size()>0)
//					new TmpObjectResourcesDB().save(objects, distributionID );
//				if(subjects.size()>0)				
//					new TmpSubjectResourcesDB().save(subjects, distributionID);
//				subjects = new ArrayList<String>();
//				objects = new ArrayList<String>();
				
			if (totalTriplesRead % 1000000 == 0) {
				logger.info("Triples read: " + totalTriplesRead + ", time: "+t.stopTimer());
				t = new Timer();
				t.startTimer();
//			}
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
