package lodVader.threads;

import java.util.HashMap;

import lodVader.TuplePart;
import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.collections.gridFS.SuperBucket;
import lodVader.utils.NSUtils;

public class JobThread implements Runnable {
	HashMap<String, String> listOfResources;
	DistributionDataSlaveThread dataThread = null;

	NSUtils nsUtils = new NSUtils();
	Integer n;

	public JobThread(DistributionDataSlaveThread dataThread, HashMap<String, String> resources) {
		this.listOfResources = resources;
		this.dataThread = dataThread;

	}

	public void saveValidLink(String resource) {
		dataThread.addValidLink(resource);
	}

	public void saveInvalidLink(String resource) {
		dataThread.addInvalidLink(resource);
	}

	public void run() {
		boolean found = false;
		try {
			if (dataThread.distributionFilters.size() == 1) {
				GoogleBloomFilter filter = dataThread.distributionFilters.iterator().next().filter;
				if (!dataThread.tuplePart.equals(TuplePart.SUBJECT)) {
					for (String resource : listOfResources.keySet()) {
						if (filter.compare(resource)) {
							saveValidLink(resource);
						} else {
							String obj = nsUtils.getNSFromString(resource);
							if (dataThread.targetNSSet.contains(obj)) {
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
						if (dataThread.targetNSSet.contains(listOfResources.get(resource))) {
							found = false;
							for (SuperBucket s : dataThread.distributionFilters) {
								if (resource.compareTo(s.firstResource) >= 0 && resource.compareTo(s.lastResource) <= 0)
									if (s.filter.compare(resource)) {
										found = true;
										saveValidLink(resource);
										break;
									}
							}
							if (!found) {
								String obj = nsUtils.getNSFromString(resource);
								if (dataThread.targetNSSet.contains(obj)) {
									saveInvalidLink(resource);
								}
							}
						}
					}
				} else {
					for (String resource : listOfResources.keySet()) {
						found = false;
						if (dataThread.targetNSSet.contains(listOfResources.get(resource)))
							for (SuperBucket s : dataThread.distributionFilters) {
								if (resource.compareTo(s.firstResource) >= 0 && resource.compareTo(s.lastResource) <= 0)
									if (s.filter.compare(resource)) {
										saveValidLink(resource);
										break;
									}
							}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
