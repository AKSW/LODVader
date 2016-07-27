package lodVader.threads;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.utils.NSUtils;

public class LinksetExtractorThreadLDLEx implements Runnable {
	// Queue<String> listOfResources;
	ConcurrentHashMap<String, HashSet<String>> listOfResources;
	LinksetDataThreadLDLEx dataThread = null;

	NSUtils nsUtils = new NSUtils();
	Integer n;

	// monitor for wait/notify thread
	Object monitor = new Object();

	// public LinksetExtractorThreadLDLEx(LinksetDataThreadLDLEx dataThread,
	// Queue<String> resources) {
	// this.listOfResources = resources;
	// this.dataThread = dataThread;
	// }
	public LinksetExtractorThreadLDLEx(LinksetDataThreadLDLEx dataThread,
			ConcurrentHashMap<String, HashSet<String>> resources) {
		this.listOfResources = resources;
		this.dataThread = dataThread;
	}

	public void saveValidLink(String resource) {
		dataThread.addValidLink(resource);
	}

	public void run() {
		boolean found = false;

		try {
			if (dataThread.distributionFilters.size() == 1) {
				BloomFilterI filter = dataThread.distributionFilters.firstEntry().getValue().filter;
				// if (!dataThread.tuplePart.equals(TuplePart.SUBJECT)) {

				for (String ns : listOfResources.keySet()) {
					for (String resource : listOfResources.get(ns)) {
						if (filter.compare(resource)) {
							saveValidLink(resource);
						}
					}
				}
			}

			else if (dataThread.distributionFilters.size() > 1) {
				for (String ns : listOfResources.keySet()) {
					try {
						for (String resource : listOfResources.get(ns)) {
							if (dataThread.distributionFilters.floorEntry(resource).getValue().filter
									.compare(resource)) {
								saveValidLink(resource);
							}
						}
					} catch (Exception e) {
						// case there null is returned, np. Means that no filter
						// describe the "range" needed
					}

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		dataThread.isBeingConsumed.set(false);
		ProcessNSFromTupleLDLEX.numberOfWorkerActiveThreads.decrementAndGet();
	}

	// public void run() {
	// boolean found = false;
	//
	// try {
	// if (dataThread.distributionFilters.size() == 1) {
	// BloomFilterI filter =
	// dataThread.distributionFilters.firstEntry().getValue().filter;
	// // if (!dataThread.tuplePart.equals(TuplePart.SUBJECT)) {
	// while (!listOfResources.isEmpty()) {
	// String resource = listOfResources.remove();
	// if (filter.compare(resource)) {
	// saveValidLink(resource);
	// }
	// }
	//
	// }
	//
	// else if (dataThread.distributionFilters.size() > 1) {
	// while (!listOfResources.isEmpty()) {
	// try {
	// String resource = listOfResources.remove();
	// if
	// (dataThread.distributionFilters.floorEntry(resource).getValue().filter.compare(resource))
	// {
	// saveValidLink(resource);
	// }
	// } catch (Exception e) {
	// // case there null is returned, np. Means that no filter
	// // describe the "range" needed
	// }
	//
	// }
	//
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	//
	// dataThread.isBeingConsumed.set(false);
	// ProcessNSFromTupleLDLEX.numberOfWorkerActiveThreads.decrementAndGet();
	// }
}
