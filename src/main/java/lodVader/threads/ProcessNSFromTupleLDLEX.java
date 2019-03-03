package lodVader.threads;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import ldlex.seeder.NSDistributionMapperHashImpl;
import ldlex.seeder.NSDistributionMapperInterface;
import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.models.LoadedBloomFiltersCache;
import lodVader.configuration.Config;
import lodVader.configuration.LODVaderProperties;
import lodVader.enumerators.TuplePart;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.namespaces.DistributionObjectNS0DB;
import lodVader.mongodb.collections.namespaces.DistributionObjectNSDB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNS0DB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;
import lodVader.mongodb.collections.toplinks.TopValidLinks;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.utils.NSUtils;
import lodVader.utils.Timer;

public abstract class ProcessNSFromTupleLDLEX extends Thread {
	
	@Autowired
	Config conf;
	
	final static Logger logger = LoggerFactory.getLogger(ProcessNSFromTupleLDLEX.class);

	// tuple part identifies whether we are working with subject or object
	protected TuplePart tuplePart;

	// control the number of opened threads
	public static AtomicInteger numberOfWorkerActiveThreads = new AtomicInteger(0);

	public AtomicInteger numberOfMakeLinksActiveThreads = new AtomicInteger(0);

	public AtomicInteger numberOfFinishedThreads = new AtomicInteger(0);

	public AtomicInteger numberThreadsWaiting = new AtomicInteger(0);

	// map of NS and Distributions
	NSDistributionMapperInterface mapper = new NSDistributionMapperHashImpl();

	// status of the distribution: null = not loaded, 1 = loading, 2 = loaded
	static protected ConcurrentHashMap<Integer, Integer> distributionStatusSubject = new ConcurrentHashMap<Integer, Integer>();
	static protected ConcurrentHashMap<Integer, Integer> distributionStatusObject = new ConcurrentHashMap<Integer, Integer>();

	protected ConcurrentHashMap<Integer, Integer> distributionStatus = new ConcurrentHashMap<Integer, Integer>();

	protected int threadNumber = 0;

	// map of threads used to compare the source distribution with target
	// distributions
	protected ConcurrentHashMap<String, Thread> mapOfThreads = new ConcurrentHashMap<String, Thread>();

	// namespaces of the processed resources (max value is
	// LODVaderProperties.CHECK_LINKS_EACH)
	public HashSet<String> chunkOfNS = new HashSet<String>();
	public HashSet<String> chunkOfNS0 = new HashSet<String>();

	// control whether the thread is still working or not
	volatile private boolean doneSplittingString;

	// queue of resources (objects OR subjects)
	protected BlockingQueue<String> resourceQueue = null;

	// resource, NS
	protected HashMap<String, String> resourcesToBeProcessedBuffer = new HashMap<String, String>(
			LODVaderProperties.CHECK_LINKS_EACH * 2);

	// current distribution being processed
	public DistributionDB distribution;

	// map containing the distribution ID (key) and the thread (value)
	protected static ConcurrentHashMap<Integer, LinksetDataThreadLDLEx> mapOfWorkerThreadsSubject = new ConcurrentHashMap<Integer, LinksetDataThreadLDLEx>();
	protected static ConcurrentHashMap<Integer, LinksetDataThreadLDLEx> mapOfWorkerThreadsObject = new ConcurrentHashMap<Integer, LinksetDataThreadLDLEx>();
	protected ConcurrentHashMap<Integer, LinksetDataThreadLDLEx> mapOfWorkerThreads = new ConcurrentHashMap<Integer, LinksetDataThreadLDLEx>();

	// map containing all namespaces processed
	public ConcurrentHashMap<String, Integer> LoadedNSCounter = new ConcurrentHashMap<String, Integer>();
	public ConcurrentHashMap<String, Integer> mapOfAllLoadedNS0 = new ConcurrentHashMap<String, Integer>();

	// namespace counter
	private ConcurrentHashMap<String, Integer> countTotalNS = null;
	private ConcurrentHashMap<String, Integer> countTotalNS0 = null;

	int dae = 0;

	int numberOfReadedResources = 0;

	public void init(BlockingQueue<String> resourceQueue, String uri, TuplePart tuplePart)
			throws MalformedURLException {
		this.resourceQueue = resourceQueue;
		this.tuplePart = tuplePart;
		this.countTotalNS = new ConcurrentHashMap<String, Integer>();
		this.countTotalNS0 = new ConcurrentHashMap<String, Integer>();
		DistributionDB d = conf.getDistributionDB();
		d.init(uri);
		this.distribution = d;

		if (tuplePart.equals(TuplePart.OBJECT))
			mapOfWorkerThreads = mapOfWorkerThreadsObject;
		else
			mapOfWorkerThreads = mapOfWorkerThreadsSubject;

		if (tuplePart.equals(TuplePart.OBJECT))
			distributionStatus = distributionStatusObject;
		else
			distributionStatus = distributionStatusSubject;

		logger.info(mapOfWorkerThreads.size() + " distributions loaded in the buffer.");

		if (LoadedBloomFiltersCache.describedSubjectsNSCurrentSize > LODVaderProperties.BF_BUFFER_RANGE
				|| LoadedBloomFiltersCache.describedSubjectsNS == null) {
			LoadedBloomFiltersCache.describedSubjectsNS = new DistributionQueries().getDescribedNS(TuplePart.SUBJECT);
			LoadedBloomFiltersCache.describedSubjectsNSCurrentSize = 0;
		}

		if (LoadedBloomFiltersCache.describedObjectsNSCurrentSize > LODVaderProperties.BF_BUFFER_RANGE
				|| LoadedBloomFiltersCache.describedObjectsNS == null) {
			LoadedBloomFiltersCache.describedObjectsNSCurrentSize = 0;
			LoadedBloomFiltersCache.describedObjectsNS = new DistributionQueries().getDescribedNS(TuplePart.OBJECT);
		}

	}

	public boolean isDoneSplittingString() {
		return doneSplittingString;
	}

	public void setDoneSplittingString(boolean doneSplittingString) {
		this.doneSplittingString = doneSplittingString;
	}

	public void run() {

		logger.info("Starting GetDomainsFromTriplesThread class: " + tuplePart.toString());

		if (tuplePart.equals(TuplePart.OBJECT)) {
			processResource(LoadedBloomFiltersCache.describedSubjectsNS);

		} else {
			processResource(LoadedBloomFiltersCache.describedObjectsNS);
		}

		try {
			makeLinks(0);

			logger.info("Waiting all threads finish their jobs...");
			int threadMapSize = mapOfThreads.size();
			for (Thread t : mapOfThreads.values()) {
				logger.info(threadMapSize-- + " threads still running...");
				t.join();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		Timer t = new Timer();
		t.startTimer();
		logger.info("Saving links...");
		saveLinks();
		logger.debug("Time: " + t.stopTimer());

		logger.info("Saving namespaces...");
		t.startTimer();
		saveNamespaces();
		logger.debug("Time: " + t.stopTimer());

		// mapOfWorkerThreads = new ConcurrentHashMap<Integer,
		// LinksetDataThreadLDLEx>();

		logger.debug("Ending GetDomainsFromTriplesThread class.");
	}

	private void processResource(BloomFilterI describedNSFilter) {

		NSUtils nsUtils = new NSUtils();
		String ns;
		String ns0;
		String resource;
		Integer value;

		try {
			while (true) {
				// get the resource of the triple
				resource = resourceQueue.take();

				// get the namespace
				ns = nsUtils.getNSFromString(resource);

				// case there is a namespace, keep going
				if (!ns.equals("")) {

					// get ns0 (FQDN)
					ns0 = nsUtils.getNS0(resource);

					// check whether the ns is already in the set
					value = countTotalNS.get(ns);

					// case not, put it
					if (value == null)
						countTotalNS.put(ns, 1);

					// case yes, increment counter
					else
						countTotalNS.put(ns, value + 1);

					// save ns0 (no needs to count)
					countTotalNS0.put(ns0, 0);

					// if some set describes the NS, keep going
					if (describedNSFilter.compare(ns)) {

						// add the resource to a queue
						resourcesToBeProcessedBuffer.put(resource, ns0);

						// add namespaces to tmp sets
						chunkOfNS.add(ns);
						chunkOfNS0.add(ns0);
						numberOfReadedResources++;

						// case the chunk is full, make linksets!
						if (numberOfReadedResources % LODVaderProperties.CHECK_LINKS_EACH == 0) {
							try {

								makeLinks(5000);

							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}

			}
		} catch (InterruptedException e1) {
			setDoneSplittingString(true);
			// e1.printStackTrace();
			return;
		}

	}

	private void saveLinks() {
		int nrLinkedDatasets = 0;
		// remove all links previously created for this distribution
		conf.getLinksetDB().removeAllLinks(distribution.getLODVaderID());

		for (LinksetDataThreadLDLEx dataThread : mapOfWorkerThreads.values()) {
			LinksetDB linkset;

			Timer t = new Timer();
			t.startTimer();

			String linksetID;

			if (dataThread.getAllValidLinks() != null) {
				// logger.info("Saving links for " +
				// dataThread.distribution.getTitle() + "... links:
				// "+dataThread.getAllValidLinks().size());
				// System.out.println(dataThread.getAllValidLinks().keySet().iterator().next().toString());
				nrLinkedDatasets++;

				if (tuplePart.equals(TuplePart.SUBJECT)) {
					linksetID = dataThread.distributionID + "-" + distribution.getLODVaderID();
					linkset = conf.getLinksetDB();
					linkset.setLinksetID(linksetID);
					linkset.setDistributionSource(dataThread.distributionID);
					linkset.setDistributionTarget(distribution.getLODVaderID());
					linkset.setDatasetSource(dataThread.datasetID);
					linkset.setDatasetTarget(distribution.getTopDatasetID());
					linkset.setDistributionSourceIsVocabulary(dataThread.distribution.getIsVocabulary());
					linkset.setDistributionTargetIsVocabulary(distribution.getIsVocabulary());

					// save top N valid and invalid links
					TopValidLinks validLinks = new TopValidLinks();
					validLinks.saveAll(dataThread.getAllValidLinks(), dataThread.distributionID,
							distribution.getLODVaderID());
					linkset.setLinks(dataThread.getAllValidLinks().size());
					dataThread.setValidLinks(null);

				} else {
					// calculate linksets
					linksetID = distribution.getLODVaderID() + "-" + dataThread.distributionID;
					linkset = conf.getLinksetDB();
					linkset.setLinksetID(linksetID);

					linkset.setDistributionSource(distribution.getLODVaderID());
					linkset.setDistributionTarget(dataThread.distributionID);
					linkset.setDatasetSource(distribution.getLODVaderID());
					linkset.setDatasetTarget(dataThread.datasetID);
					linkset.setDistributionSourceIsVocabulary(distribution.getIsVocabulary());
					linkset.setDistributionTargetIsVocabulary(dataThread.distribution.getIsVocabulary());

					// save top N valid and invalid links
					TopValidLinks validLinks = new TopValidLinks();
					validLinks.saveAll(dataThread.getAllValidLinks(), distribution.getLODVaderID(),
							dataThread.distributionID);
					linkset.setLinks(dataThread.getAllValidLinks().size());
					dataThread.setValidLinks(null);

				}

				if (linkset.getLinks() > 0)
					linkset.db.update(true, LinksetDB.LINKSET_ID, linksetID);

				logger.debug("Saved links: " + linkset.getLinks() + " (good) " + linkset.getInvalidLinks()
						+ " (bad) in " + t.stopTimer() + "s");
			}

		}
		logger.info("Links saved with " + nrLinkedDatasets + " dataset(s).");
	}

	// external-links_en.nt
	private boolean saveNamespaces() {
		logger.info("Saving NS0...");
		if (tuplePart.equals(TuplePart.SUBJECT)) {
			conf.getDistributionSubjectNS0DB().bulkSave(countTotalNS0.keySet(), distribution);
		} else {
			conf.getDistributionObjectNS0DB().bulkSave(countTotalNS0.keySet(), distribution);
		}

		logger.info("Saving NS...");
		if (tuplePart.equals(TuplePart.SUBJECT)) {
			
			DistributionSubjectNSDB d = conf.getDistributionSubjectNSDB();
			d.bulkSave(countTotalNS, distribution);
		} else {
			DistributionObjectNSDB d = new DistributionObjectNSDB();
			d.bulkSave(countTotalNS, distribution);
		}
		return true;
	}

	public abstract void makeLinks(int treshold) throws Exception;

}
