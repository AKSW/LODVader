package lodVader.threads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import lodVader.LODVaderProperties;
import lodVader.TuplePart;
import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.linksets.DistributionFilter;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.namespaces.DistributionObjectNS0DB;
import lodVader.mongodb.collections.namespaces.DistributionObjectNSDB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNS0DB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;
import lodVader.mongodb.collections.toplinks.TopInvalidLinks;
import lodVader.mongodb.collections.toplinks.TopValidLinks;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.utils.NSUtils;
import lodVader.utils.Timer;

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

	public HashSet<String> localNS = new HashSet<String>();
	public HashSet<String> localNS0 = new HashSet<String>();

	// public GoogleBloomFilter describedSubjectsNS0 = new DistributionQueries()
	// .getDescribedNS0(LODVaderProperties.TYPE_SUBJECT);
	// public GoogleBloomFilter describedObjectsNS0 = new DistributionQueries()
	// .getDescribedNS0(LODVaderProperties.TYPE_OBJECT);

	public GoogleBloomFilter describedSubjectsNS = new DistributionQueries()
			.getDescribedNS(LODVaderProperties.TYPE_SUBJECT);
	public GoogleBloomFilter describedObjectsNS = new DistributionQueries()
			.getDescribedNS(LODVaderProperties.TYPE_OBJECT);

	private boolean doneSplittingString;

	private ConcurrentLinkedQueue<String> resourceQueue = null;

	// resource, NS
	protected HashMap<String, String> resourcesToBeProcessedQueue = new HashMap<String, String>();
	public DistributionDB distribution;
	protected ConcurrentHashMap<Integer, DataModelThread> listOfWorkerThreads = new ConcurrentHashMap<Integer, DataModelThread>();
	public ConcurrentHashMap<String, Integer> listLoadedNS = new ConcurrentHashMap<String, Integer>();
	public ConcurrentHashMap<String, Integer> listLoadedNS0 = new ConcurrentHashMap<String, Integer>();

	private ConcurrentHashMap<String, Integer> countTotalNS = null;
	private ConcurrentHashMap<String, Integer> countTotalNS0 = null;

	int numberOfReadedTriples = 0;

	// int saveDomainsEach = 30000;
	int makeLinksEach = LODVaderProperties.CHECK_LINKS_EACH;

	public HashMap<Integer, Thread> threads = new HashMap<Integer, Thread>();

	public ProcessNSFromTuple(ConcurrentLinkedQueue<String> resourceQueue, String uri) {
		this.resourceQueue = resourceQueue;
		this.countTotalNS = new ConcurrentHashMap<String, Integer>();
		this.countTotalNS0 = new ConcurrentHashMap<String, Integer>();
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
		String ns0 = "";
		String resource = "";
		while (!doneSplittingString) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			if (tuplePart.equals(LODVaderProperties.TYPE_OBJECT))
				while (resourceQueue.size() > 0) {
					resource = resourceQueue.remove();
					ns = nsUtils.getNSFromString(resource);
					ns0 = nsUtils.getNS0(resource);

					if (!ns.equals("")) {
						
						countTotalNS.putIfAbsent(ns, 0);
						countTotalNS.replace(ns, countTotalNS.get(ns) + 1);
						countTotalNS0.putIfAbsent(ns0, 0);
						if (describedSubjectsNS.compare(ns)) {
							resourcesToBeProcessedQueue.put(resource, ns);
							localNS.add(ns);
							localNS0.add(ns0);
							numberOfReadedTriples++;
							if (numberOfReadedTriples % makeLinksEach == 0) {
								// if(tuplePart.equals(TuplePart.OBJECT))
								try {
									makeLinks();
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
					}

				}

			else
				while (resourceQueue.size() > 0) {
					resource = resourceQueue.remove();
					ns = nsUtils.getNSFromString(resource);
					ns0 = nsUtils.getNS0(resource);

					if (!ns.equals("")) {
						countTotalNS.putIfAbsent(ns, 0);
						countTotalNS.replace(ns, countTotalNS.get(ns) + 1);
						countTotalNS0.putIfAbsent(ns0, 0);
						if (describedObjectsNS.compare(ns)) {
							resourcesToBeProcessedQueue.put(resource, ns);
							localNS.add(ns);
							localNS0.add(ns0);
							numberOfReadedTriples++;

							if (numberOfReadedTriples % makeLinksEach == 0) {
								// if(tuplePart.equals(TuplePart.OBJECT))
								try {
									makeLinks();
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
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

		Timer t = new Timer();
		t.startTimer();
		logger.info("Saving links...");
		saveLinks();
		logger.debug("Time: " + t.stopTimer());

		logger.info("Saving namespaces...");
		t.startTimer();
		saveNSs();
		logger.debug("Time: " + t.stopTimer());

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
			validLinks.saveAll(dataThread.getAllValidLinks(), dataThread.distributionID,
					dataThread.targetDistributionID);
			l.setLinks(dataThread.getAllValidLinks().size());
			dataThread.setValidLinks(null);

			TopInvalidLinks invalidLinks = new TopInvalidLinks();
			invalidLinks.saveAll(dataThread.getAllInvalidLinks(), dataThread.distributionID,
					dataThread.targetDistributionID);
			l.setInvalidLinks(dataThread.getAllInvalidLinks().size());
			dataThread.setInvalidLinks(null);

			if (l.getLinks() > 0 || l.getInvalidLinks() > 0)
				l.updateObject(true);

		}
	}

	private boolean saveNSs() {
		logger.debug("Saving NS0...");
		ObjectId id = new ObjectId();

		if (tuplePart.equals(TuplePart.SUBJECT)) {
			for (String s : countTotalNS0.keySet()) {
				id = new ObjectId();
				DistributionSubjectNS0DB d = new DistributionSubjectNS0DB(id.get().toString());
				try {
					d.setDistributionID(distribution.getLODVaderID());
					d.setSubjectNS0(s);
					d.updateObject(true);
				} catch (LODVaderLODGeneralException e) {
					e.printStackTrace();
				}
			}
		} else {
			for (String s : countTotalNS0.keySet()) {
				id = new ObjectId();
				DistributionObjectNS0DB d = new DistributionObjectNS0DB(id.get().toString());
				try {
					d.setDistributionID(distribution.getLODVaderID());
					d.setObjectNS0(s);
					d.updateObject(true);
				} catch (LODVaderLODGeneralException e) {
					e.printStackTrace();
				}
			}
		}

		logger.debug("Saving NS...");

		Iterator it = countTotalNS.entrySet().iterator();

		if (distributionMongoDBObject == null)
			distributionMongoDBObject = new DistributionDB(uri);

		NSUtils nsUtils = new NSUtils();

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
