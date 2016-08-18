package lodVader.tupleManager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.utils.NSUtils;
import lodVader.utils.Timer;

public class SplitAndStoreNT extends SuperTupleManager {

	final static Logger logger = LoggerFactory.getLogger(SplitAndStoreNT.class);

	BufferedWriter dumpNTFile = null;
	
	NSUtils nsUtils = new NSUtils();

	public SplitAndStoreNT(BlockingQueue<String> subjectQueue, BlockingQueue<String> objectQueue,
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

		addToMap(allPredicates,  nsUtils.getNSFromString(stPredicate));
		
		if (stPredicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
			addToMap(rdfTypeObjects, nsUtils.getNSFromString(stObject));
		}
		
		if (stSubject.startsWith("http")) {
			stSubject = "<" + stSubject + ">";
		}
		if (stPredicate.startsWith("http")) {
			stPredicate = "<" + stPredicate + ">";
		}
		if (stObject.startsWith("http")) {
			stObject = "<" + stObject + ">";
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
