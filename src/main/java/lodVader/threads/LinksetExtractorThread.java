package lodVader.threads;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.enumerators.TuplePart;
import lodVader.linksets.DatasetBloomFilterContainer;
import lodVader.utils.NSUtils;

public class LinksetExtractorThread implements Runnable {
	HashMap<String, String> listOfResources;
	LinksetDataThread dataThread = null;

	NSUtils nsUtils = new NSUtils();
	Integer n;
	ConcurrentHashMap<Integer, DatasetBloomFilterContainer> datasetResourceData;

	// monitor for wait/notify thread
	Object monitor = new Object();

	public LinksetExtractorThread(LinksetDataThread dataThread, HashMap<String, String> resources,
			ConcurrentHashMap<Integer, DatasetBloomFilterContainer> datasetResourceData) {
		this.listOfResources = resources;
		this.dataThread = dataThread;
		this.datasetResourceData = datasetResourceData;
	}

	public void saveValidLink(String resource) {
		dataThread.addValidLink(resource);
//		if (dataThread.tuplePart.equals(TuplePart.SUBJECT))
//			datasetResourceData.get(dataThread.targetDatasetID).addObject(resource);
//		else
//			datasetResourceData.get(dataThread.targetDatasetID).addSubject(resource);

	}

	public void saveInvalidLink(String resource) {
		dataThread.addInvalidLink(resource);
	}

	public void run() {
		boolean found = false;
		boolean sameDataset = false;
		if (dataThread.sourceDatasetID == dataThread.targetDatasetID)
			sameDataset = true;
		try {
			if (dataThread.distributionFilters.size() == 1) {
				BloomFilterI filter = dataThread.distributionFilters.firstEntry().getValue().filter;
				if (!dataThread.tuplePart.equals(TuplePart.SUBJECT)) {
					for (String resource : listOfResources.keySet()) {
						if (filter.compare(resource)) {
							saveValidLink(resource);
						} else if (!sameDataset) {
							String obj = nsUtils.getNSFromString(resource);
							if (dataThread.targetNSSet.compare(obj)) {
								// if (dataThread.dis) {
								saveInvalidLink(resource);
							}
						}
					}
				} else {
					for (String resource : listOfResources.keySet()) {
						if (filter.compare(resource)) {
							saveValidLink(resource);
						}
					}
				}
			}

			else if (dataThread.distributionFilters.size() > 1) {
				if (!dataThread.tuplePart.equals(TuplePart.SUBJECT)) {
					for (String resource : listOfResources.keySet()) {
						if (dataThread.targetNSSet.compare(listOfResources.get(resource))) {
							found = false;

							try {
								if (dataThread.distributionFilters.floorEntry(resource).getValue().filter
										.compare(resource)) {
									found = true;
									saveValidLink(resource);
								}
							} catch (NullPointerException e) {

							}

							// for (SuperBucket s :
							// dataThread.distributionFilters) {
							// if (resource.compareTo(s.firstResource) >= 0 &&
							// resource.compareTo(s.lastResource) <= 0)
							// if (s.filter.compare(resource)) {
							// found = true;
							// saveValidLink(resource);
							// break;
							// }
							// }
							if (!found && !sameDataset) {
								String obj = nsUtils.getNSFromString(resource);
								if (dataThread.targetNSSet.compare(obj)) {
									saveInvalidLink(resource);
								}
							}
						}
					}
				} else {
					for (String resource : listOfResources.keySet()) {
						if (dataThread.targetNSSet.compare(listOfResources.get(resource))) {
							try {
								if (dataThread.distributionFilters.floorEntry(resource).getValue().filter
										.compare(resource)) {
									saveValidLink(resource);
								}
							} catch (NullPointerException e) {

							}
						}
						// for (SuperBucket s : dataThread.distributionFilters)
						// {
						// if (resource.compareTo(s.firstResource) >= 0 &&
						// resource.compareTo(s.lastResource) <= 0)
						// if (s.filter.compare(resource)) {
						// saveValidLink(resource);
						// break;
						// }
						// }
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
