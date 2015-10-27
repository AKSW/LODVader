package lodVader.threads;

import java.util.HashMap;

import lodVader.TuplePart;
import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.collections.gridFS.SuperBucket;
import lodVader.utils.NSUtils;

public class JobThread implements Runnable {
	HashMap<String, String> listOfResources;
	DataModelThread dataThread = null;

	NSUtils nsUtils = new NSUtils();
	Integer n;

	public JobThread(DataModelThread dataThread, HashMap<String, String> resources) {
		this.listOfResources = resources;
		this.dataThread = dataThread;

	}

	public void saveValidLink(String resource) {
		n = dataThread.validLinks.get(resource);
		if (n != null)
			dataThread.validLinks.putIfAbsent(resource, n + 1);
		else
			dataThread.validLinks.putIfAbsent(resource, 1);
	}

	public void saveInvalidLink(String resource) {
		n = dataThread.validLinks.get(resource);
		if (n != null)
			dataThread.invalidLinks.putIfAbsent(resource, n + 1);
		else
			dataThread.invalidLinks.putIfAbsent(resource, 1);
	}

	public void run() {
		boolean isSubject = false;
		if (dataThread.tuplePart.equals(TuplePart.SUBJECT))
			isSubject = true;
		boolean found = false;
		int bucketSize = dataThread.filters.size();
		
//		if(dataThread.targetDistributionID == 33135)
		try {

			if (dataThread.filters.size() == 1) {
				GoogleBloomFilter filter = dataThread.filters.iterator().next().filter;
				if (!isSubject) {
					for (String resource : listOfResources.keySet()) {
						if (filter.compare(resource)) {
							// dataThread.numberOfValidLinks.addAndGet(1);
							saveValidLink(resource);
						} else {
							String obj = nsUtils.getNSFromString(resource);
							if (dataThread.targetNSSet.contains(obj)) {
								// case math with tree values, add invalid link
								// by 1
								// dataThread.numberOfInvalidLinks.addAndGet(1);
								saveInvalidLink(resource);
							}
						}
					}
				} else {
					for (String resource : listOfResources.keySet()) {
						if (filter.compare(resource)) {
//							dataThread.numberOfValidLinks.addAndGet(1);
							saveValidLink(resource);
						}
					}
				}
			}

			else if (dataThread.filters.size() > 1) {
				if (!isSubject) {
					for (String resource : listOfResources.keySet()) {
						if (dataThread.targetNSSet.contains(listOfResources.get(resource))) {
							found = false;
							for (SuperBucket s : dataThread.filters) {
								if (resource.compareTo(s.firstResource) >= 0 && resource.compareTo(s.lastResource) <= 0)
									if (s.filter.compare(resource)) {
										found = true;
										// dataThread.numberOfValidLinks.addAndGet(1);
										saveValidLink(resource);
										break;
									}
							}

							if (!found) {

								String obj = nsUtils.getNSFromString(resource);
								if (dataThread.targetNSSet.contains(obj)) {
									// case math with tree values, add invalid
									// link
									// by 1
									// dataThread.numberOfInvalidLinks.addAndGet(1);
									saveInvalidLink(resource);
									// if(!dataThread.isSubject)
									// System.out.println(obj);
									// System.out.println(resource);
								}
							}
						}
					}
				} else {
					for (String resource : listOfResources.keySet()) {
						found = false;
						if (dataThread.targetNSSet.contains(listOfResources.get(resource)))
							for (SuperBucket s : dataThread.filters) {
								if (resource.compareTo(s.firstResource) >= 0 && resource.compareTo(s.lastResource) <= 0)
									if (s.filter.compare(resource)) {
//										dataThread.numberOfValidLinks.addAndGet(1);
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

		// try {
		// for (String resource : listOfResources) {
		//
		// found = false;
		//
		// if (bucketSize > 1)
		// for (SuperBucket s : dataThread.filters) {
		// if (resource.compareTo(s.firstResource) >= 0 &&
		// resource.compareTo(s.lastResource) <= 0)
		// if (s.filter.compare(resource)) {
		// found = true;
		// break;
		// }
		// }
		// else if
		// (dataThread.filters.iterator().next().filter.compare(resource))
		// found = true;
		//
		// if (found)
		// dataThread.links.addAndGet(1);
		// else {
		// if (!isSubject) {
		// String obj = nsUtils.getNSFromString(resource);
		// if (dataThread.targetNSTree.contains(obj)) {
		// // case math with tree values, add invalid link by 1
		// dataThread.invalidLinks.addAndGet(1);
		// // if(!dataThread.isSubject)
		// // System.out.println(obj);
		// // System.out.println(resource);
		// }
		// }
		// }
		// }
		//
		// // if(isSubject){
		// // String obj;
		// // get FQDN of value to compare
		// // String[] ar = resource.split("/");
		// // if (ar.length > 5)
		// // obj = ar[0] + "//" + ar[2] + "/" + ar[3] + "/" + ar[4] + "/"+
		// // ar[5] + "/";
		// // if (ar.length > 4)
		// // obj = ar[0] + "//" + ar[2] + "/" + ar[3] + "/" + ar[4] + "/";
		// // if (ar.length > 3)
		// // obj = ar[0] + "//" + ar[2] + "/" + ar[3] + "/";
		// // else if (ar.length > 2)
		// // obj = ar[0] + "//" + ar[2] + "/";
		// // else {
		// // obj = null;
		// // }
		//
		// // obj = nsUtils.getNSFromString(resource);
		// //
		// // if(obj!=null){
		// // // compare with tree
		// // if(dataThread.targetNSTree.contains(obj)){
		// // // case math with tree values, add invalid link by 1
		// // dataThread.invalidLinks.addAndGet(1);
		// //// if(!dataThread.isSubject)
		// // }
		// //
		// // }
		// //
		// // }
		// // }
		// // }
		//
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		ProcessNSFromTuple.numberOfOpenThreads.decrementAndGet();
	}
}
