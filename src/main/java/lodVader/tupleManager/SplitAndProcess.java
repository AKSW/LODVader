package lodVader.tupleManager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.utils.Timer;

public class SplitAndProcess extends SuperTupleManager {

	final static Logger logger = LoggerFactory.getLogger(SplitAndProcess.class);

	BufferedWriter subjectFile = null;
	BufferedWriter objectFile = null;

	private String lastSubject = "";

	public SplitAndProcess(ConcurrentLinkedQueue<String> subjectQueue, ConcurrentLinkedQueue<String> objectQueue,
			String fileName, DistributionDB distribution) {
		this.objectQueue = objectQueue;
		this.subjectQueue = subjectQueue;
		this.fileName = fileName;
		this.distribution = distribution;

		startFiles();
	}

	@Override
	public void startFiles() {
		try {
			if (subjectQueue != null) {
				subjectFile = new BufferedWriter(
						new FileWriter(LODVaderProperties.SUBJECT_FILE_DISTRIBUTION_PATH + fileName));
				distribution.setSubjectFile(LODVaderProperties.SUBJECT_PATH+fileName);
			}
			if (objectQueue != null){
				objectFile = new BufferedWriter(
						new FileWriter(LODVaderProperties.OBJECT_FILE_DISTRIBUTION_PATH + fileName));
				distribution.setObjectFile(LODVaderProperties.OBJECT_PATH+fileName);
			}
			distribution.update(true);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void closeFiles() {
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

	@Override
	public void saveStatement(String stSubject, String stPredicate, String stObject) {

		if (stObject.startsWith("<")) {
			stObject = stObject.substring(1, stObject.length() - 1);
			if (stObject.endsWith(">"))
				stObject = stObject.substring(0, stObject.length() - 1);

		}

		// http://www.w3.org/1999/02/22-rdf-syntax-ns#
		// if(stPredicate.equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#>")){
		// save rdf:type

		addToMap(allPredicates, stPredicate);

		if (stObject.equals("http://www.w3.org/2002/07/owl#Class")) {
			addToMap(owlClasses, stSubject);
		} else if (stPredicate.equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {
			addToMap(rdfSubClassOf, stObject);
		} else if (stPredicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
			// addToMap(rdfTypeSubjects, triple.getSubject().toString());
			addToMap(rdfTypeObjects, stObject);
		}

		// save predicate

		try {

			// compare the current subject with the previous one
			if (!stSubject.equals(lastSubject)) {

				if (stSubject.startsWith("h")) {
					// get subject and save to file
					subjectFile.write(stSubject + "\n");
					subjectLines++;
					subjectQueue.add(stSubject);
					lastSubject = stSubject;
				}

			}

			// get object (make sure that its a resource and not a literal), add
			// to queue and save to file
			if (stObject.startsWith("h")) {
				objectFile.write(stObject + "\n");
				objectQueue.add(stObject);
				objectLines++;
			}
			while (objectQueue.size() > bufferSize) {
				Thread.sleep(5);
			}
			while (subjectQueue.size() > bufferSize) {
				Thread.sleep(5);
			}

			if (totalTriplesRead % 1000000 == 0) {
				logger.info("Triples read: " + totalTriplesRead + ", time: " + t.stopTimer());
				t = new Timer();
				t.startTimer();
				logger.info("Sample subject: " + stSubject);
				logger.info("Sample predcate: " + stPredicate);
				logger.info("Sample object: " + stObject);

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
