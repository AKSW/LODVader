package lodVader.threads;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.LODVaderProperties;
import lodVader.LoadedBloomFiltersCache;
import lodVader.TuplePart;
import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.invalidLinks.InvalidLinksFilters;
import lodVader.linksets.DatasetResourcesData;
import lodVader.linksets.DistributionResourcesData;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DatasetLinksetDB;
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

public abstract class ProcessNSFromTuple extends Thread {
	final static Logger logger = LoggerFactory.getLogger(ProcessNSFromTuple.class);

	// tuple part identifies whether we are working with subject or object
	public String tuplePart;

	// map of distributions with all NS and resources (subject or object)
	public ConcurrentHashMap<Integer, DistributionResourcesData> distributionsResourceData = new ConcurrentHashMap<Integer, DistributionResourcesData>();

	// map of dataset with all resources (subject or object)
	// this is used in order to count each resource only once
	public ConcurrentHashMap<Integer, DatasetResourcesData> datasetResourceData = new ConcurrentHashMap<Integer, DatasetResourcesData>();

	protected int threadNumber = 0;
	
	public double unknownLinks = 0;

	// map of threads used to compare the source distribution with target
	// distributions
	protected ConcurrentHashMap<String, Thread> mapOfThreads = new ConcurrentHashMap<String, Thread>();

	// namespaces of the processed resources (max value is
	// LODVaderProperties.CHECK_LINKS_EACH)
	public HashSet<String> chunkOfNS = new HashSet<String>();
	public HashSet<String> chunkOfNS0 = new HashSet<String>();

	// control whether the thread is still working or not
	private boolean doneSplittingString;

	// queue of resources (objects OR subjects)
	private ConcurrentLinkedQueue<String> resourceQueue = null;

	// resource, NS
	protected HashMap<String, String> resourcesToBeProcessedQueue = new HashMap<String, String>();

	// current distribution being processed
	public DistributionDB distribution;

	// map containing the distribution ID (key) and the thread (value)
	protected ConcurrentHashMap<Integer, LinksetDataThread> mapOfWorkerThreads = new ConcurrentHashMap<Integer, LinksetDataThread>();

	// map containing all namespaces processed
	public ConcurrentHashMap<String, Integer> mapOfAllLoadedNS = new ConcurrentHashMap<String, Integer>();
	public ConcurrentHashMap<String, Integer> mapOfAllLoadedNS0 = new ConcurrentHashMap<String, Integer>();

	// namespace counter
	private ConcurrentHashMap<String, Integer> countTotalNS = null;
	private ConcurrentHashMap<String, Integer> countTotalNS0 = null;

	int numberOfReadedResources = 0;

	public ProcessNSFromTuple(ConcurrentLinkedQueue<String> resourceQueue, String uri) throws MalformedURLException {
		this.resourceQueue = resourceQueue;
		this.countTotalNS = new ConcurrentHashMap<String, Integer>();
		this.countTotalNS0 = new ConcurrentHashMap<String, Integer>();
		this.distribution = new DistributionDB(uri);

		if (LoadedBloomFiltersCache.describedSubjectsNSCurrentSize > LODVaderProperties.BF_BUFFER_RANGE
				|| LoadedBloomFiltersCache.describedSubjectsNS == null) {
			LoadedBloomFiltersCache.describedSubjectsNS = new DistributionQueries()
					.getDescribedNS(LODVaderProperties.TYPE_SUBJECT);
			LoadedBloomFiltersCache.describedSubjectsNSCurrentSize = 0;
		}

		if (LoadedBloomFiltersCache.describedObjectsNSCurrentSize > LODVaderProperties.BF_BUFFER_RANGE
				|| LoadedBloomFiltersCache.describedObjectsNS == null) {
			LoadedBloomFiltersCache.describedObjectsNSCurrentSize = 0;
			LoadedBloomFiltersCache.describedObjectsNS = new DistributionQueries()
					.getDescribedNS(LODVaderProperties.TYPE_OBJECT);
		}

	}

	public boolean isDoneSplittingString() {
		return doneSplittingString;
	}

	public void setDoneSplittingString(boolean doneSplittingString) {
		this.doneSplittingString = doneSplittingString;
	}

	public synchronized void run() {

		logger.debug("Starting GetDomainsFromTriplesThread class.");

		while (!doneSplittingString) {
			try {
				Thread.sleep(5);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			if (tuplePart.equals(LODVaderProperties.TYPE_OBJECT)) {
				processResource(LoadedBloomFiltersCache.describedSubjectsNS);

			} else {
				processResource(LoadedBloomFiltersCache.describedObjectsNS);
			}
		}

		try {
			makeLinks();

			logger.info("Waiting all threads finish their jobs...");
			for (Thread t : mapOfThreads.values()) {
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

		mapOfWorkerThreads = new ConcurrentHashMap<Integer, LinksetDataThread>();

		logger.debug("Ending GetDomainsFromTriplesThread class.");
	}

	private void processResource(GoogleBloomFilter describedNSFilter) {

		NSUtils nsUtils = new NSUtils();
		String ns;
		String ns0;
		String resource;
		Integer value;

		while (resourceQueue.size() > 0) {
			resource = resourceQueue.remove();
			ns = nsUtils.getNSFromString(resource);
			if (!ns.equals("")) {
				ns0 = nsUtils.getNS0(resource);

				value = countTotalNS.get(ns);

				if (value == null)
					countTotalNS.put(ns, 1);
				else
					countTotalNS.put(ns, value + 1);

				countTotalNS0.put(ns0, 0);

				if (describedNSFilter.compare(ns)) {
					resourcesToBeProcessedQueue.put(resource, ns);
					chunkOfNS.add(ns);
					chunkOfNS0.add(ns0);
					numberOfReadedResources++;

					if (numberOfReadedResources % LODVaderProperties.CHECK_LINKS_EACH == 0) {
						try {
							makeLinks();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				else{
					unknownLinks++;
				}
			}
		}
	}

	private void saveLinks() {
		new LinksetDB().removeAllLinks(distribution.getLODVaderID());

		for (LinksetDataThread dataThread : mapOfWorkerThreads.values()) {
			logger.debug("Saving links for " + dataThread.targetDistributionTitle);
			LinksetDB linkset;
			
			DatasetLinksetDB datasetLinkset;

			Timer t = new Timer();
			t.startTimer();

			String linksetID;

			if (tuplePart.equals(TuplePart.SUBJECT)) {
				linksetID = dataThread.targetDistributionID + "-" + dataThread.sourceDistributionID;
				linkset = new LinksetDB(linksetID);
				linkset.setDistributionSource(dataThread.targetDistributionID);
				linkset.setDistributionTarget(dataThread.sourceDistributionID);
				linkset.setDatasetSource(dataThread.targetDatasetID);
				linkset.setDatasetTarget(dataThread.sourceDatasetID); 
				linkset.setDistributionSourceIsVocabulary(new DistributionDB(dataThread.targetDistributionID).getIsVocabulary());
				linkset.setDistributionTargetIsVocabulary(new DistributionDB(dataThread.sourceDistributionID).getIsVocabulary());

				// save top N valid and invalid links
				TopValidLinks validLinks = new TopValidLinks();
				validLinks.saveAll(dataThread.getAllValidLinks(), dataThread.targetDistributionID,
						dataThread.sourceDistributionID);
				linkset.setLinks(dataThread.getAllValidLinks().size());
				dataThread.setValidLinks(null);

				TopInvalidLinks invalidLinks = new TopInvalidLinks();

				// check wheter links are described in the target distribution
				HashMap<String, Integer> invalidLinksMap = dataThread.getAllInvalidLinks();
				HashMap<String, Integer> invalidLinksMapFinal = new HashMap<String, Integer>();

				// check if the link is invalid comparing with the whole
				// dataset, and not only with distributions
//				InvalidLinksFilters invalidLinksFilter = new InvalidLinksFilters();
//				invalidLinksFilter.loadDatasetObjectFilter(linkset.getDatasetTarget());

				for (String link : invalidLinksMap.keySet()) {
					if (!datasetResourceData.get(dataThread.targetDatasetID).queryObject(link))
							invalidLinksMapFinal.put(link, invalidLinksMap.get(link));
				}

//				invalidLinksFilter = null;
				invalidLinks.saveAll(invalidLinksMapFinal, dataThread.targetDistributionID,
						dataThread.sourceDistributionID);
				linkset.setInvalidLinks(invalidLinksMapFinal.size());
				dataThread.setInvalidLinks(null);

			} else {
				// only calculate links from distribution to dataset if we are analysing objects -> subjects
				String datasetLinksetID =  dataThread.sourceDistributionID + "-" + dataThread.targetDatasetID;
				
				datasetLinkset = new DatasetLinksetDB(datasetLinksetID);
				datasetLinkset.setDistributionSource(dataThread.sourceDistributionID);
				datasetLinkset.setDistributionTarget(dataThread.targetDistributionID);
				datasetLinkset.setDatasetSource(dataThread.sourceDatasetID);
				datasetLinkset.setDatasetTarget(dataThread.targetDatasetID);
				datasetLinkset.setDistributionSourceIsVocabulary(new DistributionDB(dataThread.sourceDistributionID).getIsVocabulary());
				datasetLinkset.setDistributionTargetIsVocabulary(new DistributionDB(dataThread.targetDistributionID).getIsVocabulary());

				int linksBetweenDistAndDataset = 0;
				int deadLinksBetweenDistAndDataset = 0;
				
				// calculate linksets
				linksetID = dataThread.sourceDistributionID + "-" + dataThread.targetDistributionID;
				linkset = new LinksetDB(linksetID);

				linkset.setDistributionSource(dataThread.sourceDistributionID);
				linkset.setDistributionTarget(dataThread.targetDistributionID);
				linkset.setDatasetSource(dataThread.sourceDatasetID);
				linkset.setDatasetTarget(dataThread.targetDatasetID);
				linkset.setDistributionSourceIsVocabulary(new DistributionDB(dataThread.sourceDistributionID).getIsVocabulary());
				linkset.setDistributionTargetIsVocabulary(new DistributionDB(dataThread.targetDistributionID).getIsVocabulary());

				
				
				// save unknown links
//				distribution.setUndefinedLinks(unknownLinks);
//				try {
//					distribution.update(false);
//				} catch (LODVaderMissingPropertiesException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				} catch (LODVaderObjectAlreadyExistsException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				} catch (LODVaderNoPKFoundException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				

				// save top N valid and invalid links
				TopValidLinks validLinks = new TopValidLinks();
				validLinks.saveAll(dataThread.getAllValidLinks(), dataThread.sourceDistributionID,
						dataThread.targetDistributionID);
				linkset.setLinks(dataThread.getAllValidLinks().size());
				dataThread.setValidLinks(null);

				TopInvalidLinks invalidLinks = new TopInvalidLinks();
				// check whether links are described in the target dataset
				HashMap<String, Integer> invalidLinksMap = dataThread.getAllInvalidLinks();
				HashMap<String, Integer> invalidLinksMapFinal = new HashMap<String, Integer>();

				// check if the link is invalid comparing with the whole
				// dataset, and not only with distributions
				InvalidLinksFilters invalidLinksFilter = new InvalidLinksFilters();
				invalidLinksFilter.loadDatasetSubjectFilter(linkset.getDatasetTarget());

				logger.info("Comparing links with dataset: "+ new DatasetDB(linkset.getDatasetTarget()).getTitle());
				for (String link : invalidLinksMap.keySet()) {
					if (!datasetResourceData.get(dataThread.targetDatasetID).querySubject(link)) {
						if (!invalidLinksFilter.queryDatasetSubject(link, linkset.getDatasetTarget())){
							invalidLinksMapFinal.put(link, invalidLinksMap.get(link));
							deadLinksBetweenDistAndDataset ++;
						}
						else
							linksBetweenDistAndDataset ++;
					}
				}
				
				invalidLinksFilter = null;

				datasetLinkset.setLinks(linksBetweenDistAndDataset);
				datasetLinkset.setDeadLinks(deadLinksBetweenDistAndDataset);
				
				datasetLinkset.update(true, DatasetLinksetDB.LINKSET_ID, datasetLinksetID);
				
				// save links
				invalidLinks.saveAll(invalidLinksMapFinal, dataThread.sourceDistributionID,
						dataThread.targetDistributionID);
				linkset.setInvalidLinks(invalidLinksMapFinal.size());
				dataThread.setInvalidLinks(null);

			}

			if (linkset.getLinks() > 0 || linkset.getInvalidLinks() > 0)
				linkset.update(true, LinksetDB.LINKSET_ID, linksetID);

			logger.debug("Saved links: " + linkset.getLinks() + " (good) " + linkset.getInvalidLinks() + " (bad) in "
					+ t.stopTimer() + "s");
		}
	}

	// external-links_en.nt
	private boolean saveNamespaces() {
		logger.info("Saving NS0...");
		if (tuplePart.equals(TuplePart.SUBJECT)) {
			new DistributionSubjectNS0DB().bulkSave(countTotalNS0.keySet(), distribution);
		} else {
			new DistributionObjectNS0DB().bulkSave(countTotalNS0.keySet(), distribution);
		}

		logger.info("Saving NS...");
		if (tuplePart.equals(TuplePart.SUBJECT)) {
			DistributionSubjectNSDB d = new DistributionSubjectNSDB();
			d.bulkSave(countTotalNS, distribution);
		} else {
			DistributionObjectNSDB d = new DistributionObjectNSDB();
			d.bulkSave(countTotalNS, distribution);
		}
		return true;
	}

	public abstract void makeLinks() throws Exception;

}
