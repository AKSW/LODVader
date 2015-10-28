package lodVader.threads;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import lodVader.LODVaderProperties;
import lodVader.TuplePart;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.linksets.DistributionFilter;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.DistributionObjectNSDB;
import lodVader.mongodb.collections.DistributionSubjectNSDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.toplinks.TopInvalidLinks;
import lodVader.mongodb.collections.toplinks.TopValidLinks;
import lodVader.utils.NSUtils;

public class ProcessNSFromTuple extends Thread {
	final static Logger logger = Logger.getLogger(ProcessNSFromTuple.class);

	// public boolean isSubject = false;

	public String tuplePart;

	public int threshold = 1;

	// contains all NF described by distribution
	// <NS, <list of distribution that describes this NS>>
	public ConcurrentHashMap<Integer, DistributionFilter> distributionFilter = new ConcurrentHashMap<Integer, DistributionFilter>();

	protected int threadNumber = 0;

	protected ConcurrentHashMap<String, Thread> listOfThreads = new ConcurrentHashMap<String, Thread>();

	private String uri;
	public DistributionDB distributionMongoDBObject = null;

	public HashMap<String, Integer> localNS = new HashMap<String, Integer>();

	private boolean doneSplittingString;

	private ConcurrentLinkedQueue<String> resourceQueue = null;

	// resource, NS
	protected HashMap<String, String> resourcesToBeProcessedQueue = new HashMap<String, String>();
	public DistributionDB distribution;
	protected ConcurrentHashMap<Integer, DataModelThread> listOfWorkerThreads = new ConcurrentHashMap<Integer, DataModelThread>();
	public ConcurrentHashMap<String, Integer> listLoadedNS = new ConcurrentHashMap<String, Integer>();

	private ConcurrentHashMap<String, Integer> countTotalNS = null;

	int numberOfReadedTriples = 0;
	
	public static AtomicInteger numberOfOpenThreads = new AtomicInteger(0);

	// int saveDomainsEach = 30000;
	int makeLinksEach = LODVaderProperties.CHECK_LINKS_EACH;
	
	public HashMap<Integer, Thread> threads = new HashMap<Integer, Thread>();

	public ProcessNSFromTuple(ConcurrentLinkedQueue<String> resourceQueue, String uri) {
		this.resourceQueue = resourceQueue;
		this.countTotalNS = new ConcurrentHashMap<String, Integer>();
		this.uri = uri;
		this.distribution = new DistributionDB(uri);

	}

	public boolean isDoneSplittingString() {
		return doneSplittingString;
	}

	public void setDoneSplittingString(boolean doneSplittingString) {
		this.doneSplittingString = doneSplittingString;
	}

	public synchronized void run() {

		logger.debug("Starting GetDomainsFromTriplesThread class.");

		NSUtils nsUtils = new NSUtils();

		String ns = "";
		String resource = "";
		while (!doneSplittingString) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			while (resourceQueue.size() > 0) {
				numberOfReadedTriples++;
				try {
					resource = resourceQueue.remove();
					ns = nsUtils.getNSFromString(resource);
					resourcesToBeProcessedQueue.put(resource, ns);
					// System.out.println(" -- -
					// -"+nsUtils.getNSFromString(obj));
					// System.out.println(" -- - -"+(obj));

					if (!ns.equals("")) {
						countTotalNS.putIfAbsent(ns, 0);
						countTotalNS.replace(ns, countTotalNS.get(ns) + 1);
						localNS.put(ns, 0);

					}
					if (numberOfReadedTriples % makeLinksEach == 0) {
						// if(tuplePart.equals(TuplePart.OBJECT))
						makeLinks();
					}

				} catch (NoSuchElementException e) {
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		logger.info("Waiting all threads finish their jobs...");
		try {
			// if(tuplePart.equals(TuplePart.OBJECT))
			makeLinks();
			for (Thread t : listOfThreads.values()) {
				t.join();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		saveLinks();
		saveNSs();
		listOfWorkerThreads = new ConcurrentHashMap<Integer, DataModelThread>();

		logger.debug("Ending GetDomainsFromTriplesThread class.");
	}

	private void saveLinks() {
		for (DataModelThread dataThread : listOfWorkerThreads.values()) {

			LinksetDB l;

			String mongoDBURL;

			// System.out.println(" Links working: "+positive + "

			if (tuplePart.equals(TuplePart.SUBJECT)) {
				mongoDBURL = dataThread.targetDistributionID + "-" + dataThread.distributionID;
				l = new LinksetDB(mongoDBURL);
				l.setDistributionSource(dataThread.targetDistributionID);
				l.setDistributionTarget(dataThread.distributionID);
				l.setDatasetSource(dataThread.targetDatasetID);
				l.setDatasetTarget(dataThread.datasetID);

			} else {
				mongoDBURL = dataThread.distributionID + "-" + dataThread.targetDistributionID;
				l = new LinksetDB(mongoDBURL);

				l.setDistributionSource(dataThread.distributionID);
				l.setDistributionTarget(dataThread.targetDistributionID);
				l.setDatasetSource(dataThread.datasetID);
				l.setDatasetTarget(dataThread.targetDatasetID);
			}
			
			// save top N valid and invalid links
			TopValidLinks validLinks = new TopValidLinks();
			validLinks.saveAll(dataThread.getAllValidLinks(), dataThread.distributionID, dataThread.targetDistributionID);
			TopInvalidLinks invalidLinks = new TopInvalidLinks();
			invalidLinks.saveAll(dataThread.getAllInvalidLinks(), dataThread.distributionID, dataThread.targetDistributionID);
			
			l.setLinks(dataThread.getAllValidLinks().size());
			l.setInvalidLinks(dataThread.getAllInvalidLinks().size());
			if (l.getLinks() > 0 || l.getInvalidLinks() > 0)
				l.updateObject(true);
		}
	}

	private boolean saveNSs() {
		logger.debug("Saving domains...");
		ObjectId id = new ObjectId();

		Iterator it = countTotalNS.entrySet().iterator();

		if (distributionMongoDBObject == null)
			distributionMongoDBObject = new DistributionDB(uri);

		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next(); 
			String d = (String) pair.getKey();
			int count = (Integer) pair.getValue();
			// distributionMongoDBObj.addAuthorityObjects(d);

			if (count > threshold) {
				id = new ObjectId();
				if (tuplePart.equals(TuplePart.SUBJECT)) {
					DistributionSubjectNSDB d2 = new DistributionSubjectNSDB(id.get().toString());
					d2.setSubjectNS(d);
					d2.setDistributionID(distributionMongoDBObject.getLODVaderID());
					d2.setNumberOfResources(count);

					try {
						d2.updateObject(true);

					} catch (LODVaderLODGeneralException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {

					DistributionObjectNSDB d2 = null;
					d2 = new DistributionObjectNSDB(id.get().toString());
					d2.setObjectNS(d);
					d2.setNumberOfResources(count);
					d2.setDistributionID(distributionMongoDBObject.getLODVaderID());

					try {
						d2.updateObject(true);
					} catch (LODVaderLODGeneralException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

			}
		}

		return true;
	}

	public void makeLinks() throws Exception {
		throw new Exception("You have to implement this method.");
	}

}
