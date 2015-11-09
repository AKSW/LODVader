package lodVader.linksets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.TuplePart;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.threads.DataModelThread;
import lodVader.threads.JobThread;
import lodVader.threads.ProcessNSFromTuple;
import lodVader.utils.NSUtils;
import lodVader.utils.Timer;

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

	final static Logger logger = LoggerFactory.getLogger(MakeLinksetsMasterThread.class);

	ArrayList<DistributionDB> distributionsToCompare;
	HashMap<String,String> resourcesToBeProcessedQueueCopy;

	public HashSet<String> localNS0Copy = new HashSet<String>(); 
	public HashSet<String> localNSCopy = new HashSet<String>(); 

	@Override
	public void makeLinks() {

		localNSCopy = (HashSet<String>) localNS.clone();
		localNS0Copy = (HashSet<String>) localNS0.clone();
		resourcesToBeProcessedQueueCopy = (HashMap<String,String>) resourcesToBeProcessedQueue.clone();
//		localNS = new HashSet<String>();
		localNS0 = new HashSet<String>();
		localNS = new HashSet<String>();
		resourcesToBeProcessedQueue = new HashMap<String,String>();
		
		try {
			Thread t = new Thread(new Runnable() {
				public void run() {

					NSUtils nsUtils =  new NSUtils();
					ArrayList<String> nsToSearch = new ArrayList<String>();
					int numberOfOpenThreads = 0;

					
					// create a list of NS which should be fetched from
					// database
					// and add the loaded NS to a global map
					for (String ns0 : localNS0Copy) {
						if (!listLoadedNS.containsKey(ns0)) {
							nsToSearch.add(ns0);
							listLoadedNS.put(ns0, 0);
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
										listOfWorkerThreads.put(distributionToCompare.getLODVaderID(),
												workerThread);
										workerThread = listOfWorkerThreads.get(distributionToCompare.getLODVaderID());
										numberOfOpenThreads++;
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
							for (String ns : localNSCopy) {
								if (dNS.queryObjectNS(ns)) {
									boolean keepTrying = true;
									while (keepTrying) {
										try {
											if (!(dNS.distributionID == distribution.getLODVaderID())) {
												listOfWorkerThreads.get(dNS.distributionID).active = true;
												numberOfOpenThreads++;
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
							for (String ns : localNSCopy) {
								if (dNS.querySubjectNS(ns)) {

									boolean keepTrying = true;
									while (keepTrying) {
										try {
											if (!(dNS.distributionID == distribution.getLODVaderID())){
												listOfWorkerThreads.get(dNS.distributionID).active = true;
												numberOfOpenThreads++;
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
						}
					}

					int bufferSize = resourcesToBeProcessedQueueCopy.size();

					String[] buffer = new String[bufferSize];

					if (listOfWorkerThreads.size() > 0) {

						int threadIndex = 0;

						Thread[] threads = new Thread[listOfWorkerThreads.size()];

//						System.out.println(numberOfOpenThreads);
						
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
				}				
				
			});
			threadNumber++;
			t.setName("MakingLinksets:" + (threadNumber) + ":" + distribution.getUri());
			listOfThreads.put("MakingLinksets:" + (threadNumber) + ":" + distribution.getUri(), t);
			t.start();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
