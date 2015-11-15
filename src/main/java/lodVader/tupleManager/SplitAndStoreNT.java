package lodVader.tupleManager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.RDFHandlerBase;

import lodVader.LODVaderProperties;
import lodVader.utils.Timer;

public class SplitAndStoreNT extends SuperTupleManager {

	final static Logger logger = LoggerFactory.getLogger(SplitAndStoreNT.class);

	BufferedWriter dumpNTFile = null;

	public SplitAndStoreNT(ConcurrentLinkedQueue<String> subjectQueue, ConcurrentLinkedQueue<String> objectQueue,
			String fileName, int distributionID) {
		this.objectQueue = objectQueue;
		this.subjectQueue = subjectQueue;
		this.fileName = fileName;
		this.distributionID = distributionID;

		startFiles();
	}

	@Override
	public void startFiles() {
		try {
			if (subjectQueue != null && objectQueue != null)
				dumpNTFile = new BufferedWriter(new FileWriter(fileName));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void closeFiles() {
		try {
			if (dumpNTFile != null)
				dumpNTFile.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public void saveStatement(String stSubject, String stPredicate, String stObject) {

		if (stSubject.startsWith("http")) {
			stSubject = "<" + stSubject + ">";
		}
		if (stPredicate.startsWith("http")) {
			stPredicate = "<" + stPredicate + ">";
		}
		if (stObject.startsWith("http")) {
			stObject = "<" + stObject + ">";
		}
//		stObject = stObject + " .";

		addToMap(allPredicates, stPredicate);

		if (stObject.equals("<http://www.w3.org/2002/07/owl#Class>")) {
			addToMap(owlClasses, stSubject);
		} else if (stPredicate.equals("<http://www.w3.org/2000/01/rdf-schema#subClassOf>")) {
			addToMap(rdfSubClassOf, stObject);
		} else if (stPredicate.equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")) {
			// addToMap(rdfTypeSubjects, triple.getSubject().toString());
			addToMap(rdfTypeObjects, stObject);
		}

		try {

			dumpNTFile.write(stSubject + " " + stPredicate + " " + stObject + " ." +  "\n");

			if (totalTriplesRead % 1000000 == 0) {
				logger.info("Triples read: " + totalTriplesRead + ", time: " + t.stopTimer());
				t = new Timer();
				t.startTimer();
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
