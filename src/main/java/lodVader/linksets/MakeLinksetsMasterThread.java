package lodVader.linksets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import lodVader.TuplePart;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.threads.DataModelThread;
import lodVader.threads.ProcessNSFromTuple;
import lodVader.threads.JobThread;

public class MakeLinksetsMasterThread extends ProcessNSFromTuple {

	/**
	 * Create linksets for a distribution
	 * @param resourceQueue string queue of objects or subjects resources
	 * @param uri of the distribution (usually the distribution URL)
	 */
	public MakeLinksetsMasterThread(ConcurrentLinkedQueue<String> resourceQueue,
			String uri) {
		super(resourceQueue, uri);
	} 

	final static Logger logger = Logger.getLogger(MakeLinksetsMasterThread.class);

	ArrayList<DistributionDB> distributionsToCompare;
	ArrayList<String> resourcesToBeProcessedQueueCopy;

	public HashMap<String, Integer> localNSCopy = new HashMap<String, Integer>();

	@Override
	public void makeLinks() {

		localNSCopy = (HashMap<String, Integer>) localNS.clone();
		resourcesToBeProcessedQueueCopy = (ArrayList<String>) resourcesToBeProcessedQueue
				.clone();
		localNS = new HashMap<String, Integer>();
		resourcesToBeProcessedQueue = new ArrayList<String>();

		try {
			Thread t = new Thread(new Runnable() {
				public void run() {

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
					// list  (so we don't have to query again)
					if (tuplePart.equals(TuplePart.OBJECT))
						distributionsToCompare = new DistributionQueries()
								.getDistributionsByOutdegree(nsToSearch,
										nsPerDistribution);
					else if(tuplePart.equals(TuplePart.SUBJECT))
						distributionsToCompare = new DistributionQueries()
								.getDistributionsByIndegree(nsToSearch,
										nsPerDistribution);
					

					for (DistributionDB distributionToCompare : distributionsToCompare) {
						if (!listOfWorkerThreads
								.containsKey(distributionToCompare.getLODVaderID()))
							try {

								// check if distributions had already been
								// compared
								if (!distributionToCompare.getUri().equals(
										distribution.getUri())) {
									DataModelThread workerThread = new DataModelThread(
											distribution,
											distributionToCompare, 
											nsPerDistribution.get(distributionToCompare.getLODVaderID()),
											tuplePart);
									if (workerThread.datasetID != 0) {
										listOfWorkerThreads.putIfAbsent(
												distributionToCompare.getLODVaderID(),
												workerThread);
										workerThread = listOfWorkerThreads.get(distributionToCompare.getLODVaderID());
										workerThread.startFilter();
										
									}
								}
							} catch (Exception e) {
								logger.error("Error: "
										+ e.getMessage());
//								System.out.println(distributionToCompare);
								e.printStackTrace();
							}
					}

					for (DistributionNS dNS : nsPerDistribution.values()) {
						// check whether NS is in the subject list
						if (tuplePart.equals(TuplePart.SUBJECT)) {
							for (String ns : localNSCopy.keySet()) {
								if (dNS.hasObjectNS(ns)) { 
									boolean keepTrying = true;
									while(keepTrying){
										try{
											if(! (dNS.distribution == distribution.getLODVaderID())){
												listOfWorkerThreads.get(dNS.distribution).active = true;
											}
											keepTrying = false;
										}
										catch (Exception e){
//											e.printStackTrace();
											try {
												Thread.sleep(1);
											} catch (InterruptedException e1) {
												// TODO Auto-generated catch block
											}
										}
									}
								}
							}
						} else if (tuplePart.equals(TuplePart.OBJECT)) {
							for (String ns : localNSCopy.keySet()) {
								if (dNS.hasSubjectNS(ns)) {
									
									boolean keepTrying = true;
									while(keepTrying){
										try{
											if(!(dNS.distribution == distribution.getLODVaderID()))
												listOfWorkerThreads.get(dNS.distribution).active = true;
											
											keepTrying = false;
										}
										catch (Exception e){
//											e.printStackTrace();											
											try {
												Thread.sleep(1);
											} catch (InterruptedException e1) {
												// TODO Auto-generated catch block
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
						for (DataModelThread dataThread2 : listOfWorkerThreads
								.values()) {
							if (dataThread2.active) {
								threads[threadIndex] = new Thread(
										new JobThread(
												dataThread2,
												(ArrayList<String>) resourcesToBeProcessedQueueCopy
														.clone()));
								threads[threadIndex]
										.setName("MakeLinkSetWorker-"
												+ threadIndex + dataThread2.targetDistributionID);
								threads[threadIndex].start();
								threadIndex++;
							}
						}

						// wait all threads finish 
						for (int d = 0; d < threads.length; d++)
							try {
								threads[d].join();
							} catch (InterruptedException e) {
//								e.printStackTrace();
							}
							catch (Exception e) {
								// TODO: handle exception
							}
					}

					// save linksets into mongodb
					for (DataModelThread dataThread : listOfWorkerThreads
							.values()) {

						dataThread.active = false;
						
						LinksetDB l; 

						String mongoDBURL;

						

						// System.out.println(" Links working: "+positive + "
						
						if(tuplePart.equals(TuplePart.SUBJECT)){
							 mongoDBURL = dataThread.targetDistributionID+ "-" + dataThread.distributionID;
							 l = new LinksetDB(
										mongoDBURL);
							l.setDistributionSource(dataThread.targetDistributionID);
							l.setDistributionTarget(dataThread.distributionID);
							l.setDatasetSource(dataThread.targetDatasetID);
							l.setDatasetTarget(dataThread.datasetID);
							
						}
						else{
							 mongoDBURL = dataThread.distributionID + "-"
										+ dataThread.targetDistributionID;
							 l = new LinksetDB(
										mongoDBURL);
						
						l.setDistributionSource(dataThread.distributionID);
						l.setDistributionTarget(dataThread.targetDistributionID);
						l.setDatasetSource(dataThread.datasetID);
						l.setDatasetTarget(dataThread.targetDatasetID);
						}
						l.setLinks(dataThread.links.get());
						l.setInvalidLinks(dataThread.invalidLinks.get());
						if(l.getLinks()>0 || l.getInvalidLinks()>0)
							l.updateObject(true); 
					}

				}
			});
			threadNumber++;
			t.setName("MakingLinksets:"+(threadNumber)+":"+distribution.getUri());
			listOfThreads.putIfAbsent("MakingLinksets:"+(threadNumber)+":"+distribution.getUri(), t);
			t.start();
			

		} catch (Exception e) {

		}
	}
}
