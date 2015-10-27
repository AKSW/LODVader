package lodVader.linksets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import lodVader.TuplePart;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.threads.DataModelThread;
import lodVader.threads.JobThread;
import lodVader.threads.ProcessNSFromTuple;

public class MakeLinksetsMasterThread extends ProcessNSFromTuple {

	/**
	 * Create linksets for a distribution
	 * 
	 * @param resourceQueue
	 *            string queue of objects or subjects resources
	 * @param uri
	 *            of the distribution (usually the distribution URL)
	 */
	public MakeLinksetsMasterThread(ConcurrentLinkedQueue<String> resourceQueue, String uri) {
		super(resourceQueue, uri);
	}

	final static Logger logger = Logger.getLogger(MakeLinksetsMasterThread.class);

	ArrayList<DistributionDB> distributionsToCompare;
	HashMap<String,String> resourcesToBeProcessedQueueCopy;

	public HashMap<String, Integer> localNSCopy = new HashMap<String, Integer>();

	@Override
	public void makeLinks() {

		localNSCopy = (HashMap<String, Integer>) localNS.clone();
		resourcesToBeProcessedQueueCopy = (HashMap<String,String>) resourcesToBeProcessedQueue.clone();
		localNS = new HashMap<String, Integer>();
		resourcesToBeProcessedQueue = new HashMap<String,String>();
		
		while (numberOfOpenThreads.get() > 10) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		

		try {
			Thread t = new Thread(new Runnable() {
				public void run() {

					numberOfOpenThreads.addAndGet(1);
					ArrayList<String> nsToSearch = new ArrayList<String>();

					// create a list of NS which should be fetched from
					// database
					// and add the loaded NS to a global map
					for (String ns : localNSCopy.keySet()) {
						if (!listLoadedNS.containsKey(ns)) {
							nsToSearch.add(ns);
							listLoadedNS.putIfAbsent(ns, 0);
						}
					}

					// get which distributions describe which NS and save in a
					// list (so we don't have to query again)
					if (tuplePart.equals(TuplePart.OBJECT))
						distributionsToCompare = new DistributionQueries().getDistributionsByOutdegree(nsToSearch,
								distributionFilter);

					else if (tuplePart.equals(TuplePart.SUBJECT))
						distributionsToCompare = new DistributionQueries().getDistributionsByIndegree(nsToSearch,
								distributionFilter);

					// Boolean foundFilter = false;
					//
					//
					// Iterator it =
					// resourcesToBeProcessedQueueCopy.entrySet().iterator();
					// while (it.hasNext()) {
					// Map.Entry pair = (Map.Entry)it.next();
					// String resource = pair.getKey().toString();
					// String ns = pair.getValue().toString();
					//
					//// for(String resource:
					// resourcesToBeProcessedQueueCopy.keySet()){
					//
					//
					//
					//// if(resourceToBeRemoved!=null){
					//// resourcesToBeProcessedQueueCopy.remove(resourceToBeRemoved);
					//// resourceToBeRemoved = null;
					//// }
					//// String ns =
					// resourcesToBeProcessedQueueCopy.get(resource);
					//
					// for (DistributionDB distributionToCompare :
					// distributionsToCompare) {
					// foundFilter = false;
					//
					// // check whether is there any distribution BF loaded
					// if (!listOfWorkerThreads
					// .containsKey(distributionToCompare.getLODVaderID())){
					//
					// try {
					//
					// DataModelThread dataModelThread = new DataModelThread(
					// resource,
					// distribution,
					// distributionToCompare,
					// distributionFilter.get(distributionToCompare.getLODVaderID()),
					// tuplePart);
					//
					// if(dataModelThread.filter != null){
					//// if (dataModelThread.datasetID != 0) {
					//
					// ArrayList<DataModelThread> d = new
					// ArrayList<DataModelThread>();
					// d.add(dataModelThread);
					//
					// listOfWorkerThreads.putIfAbsent(
					// distributionToCompare.getLODVaderID(),
					// d);
					// foundFilter=true;
					//
					//// }
					// }
					// else{
					// it.remove();
					//
					// break;
					// }
					//
					//
					// } catch (Exception e) {
					//// logger.error("Error: "
					//// + e.getMessage());
					//// System.out.println(distributionToCompare);
					// e.printStackTrace();
					// }
					//
					// }
					//
					//
					//
					// if(!foundFilter){
					// if (tuplePart.equals(TuplePart.SUBJECT)) {
					// if
					// (distributionFilter.get(distributionToCompare.getLODVaderID()).objectsNS.contains(ns)){
					//
					// for(DataModelThread dt:
					// listOfWorkerThreads.get(distributionToCompare.getLODVaderID())){
					// if(dt.lastResource.compareTo(resource) >=0
					// &&
					// dt.firstResource.compareTo(resource) <=0 ){
					// dt.active = true;
					// foundFilter = true;
					// break;
					// }
					// }
					// }
					// }
					// if (tuplePart.equals(TuplePart.OBJECT)) {
					//// System.out.println(ns);
					////
					//// for(String a :
					// nsPerDistribution.get(distributionToCompare.getLODVaderID()).subjectsNS){
					//// System.out.println(a);
					//// }
					//
					// if
					// (distributionFilter.get(distributionToCompare.getLODVaderID()).subjectsNS.contains(ns)){
					// for(DataModelThread dt:
					// listOfWorkerThreads.get(
					// distributionToCompare.
					// getLODVaderID())){
					// if(dt.lastResource.
					// compareTo(
					// resource) >= 0
					// &&
					// dt.firstResource.compareTo(resource) <=0 ){
					// dt.active = true;
					// foundFilter = true;
					// break;
					// }
					// }
					// }
					// }
					// }
					// // case the DataTherad is not active, we have to load a
					// new BF
					// if(!foundFilter){
					// try {
					// if
					// (!distributionToCompare.getUri().equals(distribution.getUri()))
					// {
					//
					// DataModelThread workerThread = new DataModelThread(
					// resource,
					// distribution,
					// distributionToCompare,
					// distributionFilter.get(distributionToCompare.getLODVaderID()),
					// tuplePart);
					// if(workerThread.filter != null){
					//// if (workerThread.datasetID != 0) {
					//
					//// ArrayList<DataModelThread> d = new
					// ArrayList<DataModelThread>();
					//// d.add(workerThread);
					//
					// listOfWorkerThreads.get(distributionToCompare.getLODVaderID()).add(workerThread);
					//
					//// listOfWorkerThreads.putIfAbsent(
					//// distributionToCompare.getLODVaderID(),
					//// d);
					//// workerThread =
					// listOfWorkerThreads.get(distributionToCompare.getLODVaderID());
					//// workerThread.startFilter();
					// foundFilter=true;
					//// }
					// }
					// }
					// } catch (Exception e) {
					// logger.error("Error: "
					// + e.getMessage());
					//// System.out.println(distributionToCompare);
					// e.printStackTrace();
					// }
					// }
					//
					//
					// }
					//
					// }

					for (DistributionDB distributionToCompare : distributionsToCompare) {

						if (!listOfWorkerThreads.containsKey(distributionToCompare.getLODVaderID()))
							try {

								// check if distributions had already been
								// compared
								if (!(distributionToCompare.getLODVaderID() == distribution.getLODVaderID())) {
									DataModelThread workerThread = new DataModelThread(distribution,
											distributionToCompare,
											distributionFilter.get(distributionToCompare.getLODVaderID()), tuplePart);
									if (workerThread.datasetID != 0) {
										listOfWorkerThreads.putIfAbsent(distributionToCompare.getLODVaderID(),
												workerThread);
										workerThread = listOfWorkerThreads.get(distributionToCompare.getLODVaderID());
									}
								}
							} catch (Exception e) {
								logger.error("Error: " + e.getMessage());
								System.out.println(distributionToCompare);
								e.printStackTrace();
							}
					}

					for (DistributionFilter dNS : distributionFilter.values()) {
						// check whether NS is in the subject list
						if (tuplePart.equals(TuplePart.SUBJECT)) {
							for (String ns : localNSCopy.keySet()) {
								if (dNS.queryObjectNS(ns)) {
									boolean keepTrying = true;
									while (keepTrying) {
										try {
											if (!(dNS.distributionID == distribution.getLODVaderID())) {
												listOfWorkerThreads.get(dNS.distributionID).active = true;
											}
											keepTrying = false;
										} catch (Exception e) {
											// e.printStackTrace();
											try {
												Thread.sleep(1);
											} catch (InterruptedException e1) {
												// TODO Auto-generated catch
												// block
											}
										}
									}
								}
							}
						} else if (tuplePart.equals(TuplePart.OBJECT)) {
							for (String ns : localNSCopy.keySet()) {
								if (dNS.querySubjectNS(ns)) {

									boolean keepTrying = true;
									while (keepTrying) {
										try {
											if (!(dNS.distributionID == distribution.getLODVaderID()))
												listOfWorkerThreads.get(dNS.distributionID).active = true;

											keepTrying = false;
										} catch (Exception e) {
											// e.printStackTrace();
											try {
												Thread.sleep(1);
											} catch (InterruptedException e1) {
												// TODO Auto-generated catch
												// block
											}
										}
									}
								}
							}
						}
					}

					int bufferSize = resourcesToBeProcessedQueueCopy.size();

					String[] buffer = new String[bufferSize];

					if (listOfWorkerThreads.size() > 0) {

						int threadIndex = 0;

						Thread[] threads = new Thread[listOfWorkerThreads.size()];

						// for (Integer in : listOfWorkerThreads.keySet())
						for (DataModelThread dataThread : listOfWorkerThreads.values()) {
							if (dataThread.targetDistributionID != distribution.getLODVaderID())
								if (dataThread.active) {
									if (threads[threadIndex] == null) { 
										threads[threadIndex] = new Thread(new JobThread(dataThread,
												(HashMap<String,String>) resourcesToBeProcessedQueueCopy.clone()));
										threads[threadIndex].setName("MakeLinkSetWorker-" + threadIndex + "-"
												+ dataThread.targetDistributionID);
										threads[threadIndex].start();
										threadIndex++;
//										numberOfOpenThreads.addAndGet(1);
//										
//										
//										
//										while (numberOfOpenThreads.get() > 5) {
//											try {
//												Thread.sleep(10);
//											} catch (InterruptedException e) {
//												// TODO Auto-generated catch block
//												e.printStackTrace();
//											}
//										}
										
										
									}
									else{
//										threads[threadIndex].
									}
								}
						}

						// wait all threads finish
						for (int d = 0; d < threads.length; d++)
							try {
								threads[d].join();
							} catch (InterruptedException e) {
								// e.printStackTrace();
							} catch (Exception e) {
								// TODO: handle exception
							}
					}

					// save linksets into mongodb

					for (DataModelThread dataThread : listOfWorkerThreads.values()) {
						dataThread.active = false;
					}

					// for (DataModelThread dataThread : listOfWorkerThreads
					// .values()) {
					//
					// dataThread.active = false;
					//
					// LinksetDB l;
					//
					// String mongoDBURL;
					//
					// // System.out.println(" Links working: "+positive + "
					//
					// if(tuplePart.equals(TuplePart.SUBJECT)){
					// mongoDBURL = dataThread.targetDistributionID+ "-" +
					// dataThread.distributionID;
					// l = new LinksetDB(
					// mongoDBURL);
					// l.setDistributionSource(dataThread.targetDistributionID);
					// l.setDistributionTarget(dataThread.distributionID);
					// l.setDatasetSource(dataThread.targetDatasetID);
					// l.setDatasetTarget(dataThread.datasetID);
					//
					// }
					// else{
					// mongoDBURL = dataThread.distributionID + "-"
					// + dataThread.targetDistributionID;
					// l = new LinksetDB(
					// mongoDBURL);
					//
					// l.setDistributionSource(dataThread.distributionID);
					// l.setDistributionTarget(dataThread.targetDistributionID);
					// l.setDatasetSource(dataThread.datasetID);
					// l.setDatasetTarget(dataThread.targetDatasetID);
					// }
					// l.setLinks(dataThread.numberOfValidLinks.get());
					// l.setInvalidLinks(dataThread.numberOfInvalidLinks.get());
					// if(l.getLinks()>0 || l.getInvalidLinks()>0)
					// l.updateObject(true);
					// }
					
					numberOfOpenThreads.decrementAndGet();

				}
				
				
				
			});
			threadNumber++;
			t.setName("MakingLinksets:" + (threadNumber) + ":" + distribution.getUri());
			listOfThreads.putIfAbsent("MakingLinksets:" + (threadNumber) + ":" + distribution.getUri(), t);
			t.start();

		} catch (Exception e) {

		}
	}
}
