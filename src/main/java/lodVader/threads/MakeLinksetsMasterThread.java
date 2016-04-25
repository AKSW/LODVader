package lodVader.threads;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.enumerators.TuplePart;
import lodVader.linksets.DatasetResourcesData;
import lodVader.linksets.DistributionResourcesData;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.DistributionQueries;
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
	 * @throws MalformedURLException
	 */
	public MakeLinksetsMasterThread(ConcurrentLinkedQueue<String> resourceQueue, String uri)
			throws MalformedURLException {
		super(resourceQueue, uri);
	}

	final static Logger logger = LoggerFactory.getLogger(MakeLinksetsMasterThread.class);

	ArrayList<DistributionDB> distributionsToCompare;
	HashMap<String, String> resourcesToBeProcessedQueueCopy;

	public HashSet<String> localNS0Copy;
	public HashSet<String> localNSCopy;

	@Override
	public void makeLinks() {

		localNSCopy = chunkOfNS;
		localNS0Copy = chunkOfNS0;
		resourcesToBeProcessedQueueCopy = resourcesToBeProcessed;
		// localNS = new HashSet<String>();

		chunkOfNS0 = new HashSet<String>();
		chunkOfNS = new HashSet<String>();
		resourcesToBeProcessed = new HashMap<String, String>();

		try {
			Thread t = new Thread(new Runnable() {
				public void run() {

					ArrayList<String> nsToSearch = new ArrayList<String>();

					// create a list of NS which should be fetched from
					// database
					// and add the loaded NS to a global map
					for (String ns0 : localNS0Copy) {
						if (!mapOfAllLoadedNS.containsKey(ns0)) {
							nsToSearch.add(ns0);
							mapOfAllLoadedNS.put(ns0, 0);
						}
					}

					// get which distributions describe which NS and save in a
					// list (so we don't have to query again)
					if (tuplePart.equals(TuplePart.OBJECT))
						distributionsToCompare = new DistributionQueries().getDistributionsByOutdegree(nsToSearch,
								distributionsResourceData);

					else if (tuplePart.equals(TuplePart.SUBJECT))
						distributionsToCompare = new DistributionQueries().getDistributionsByIndegree(nsToSearch,
								distributionsResourceData);

					for (DistributionDB distributionToCompare : distributionsToCompare) {

						if (!mapOfWorkerThreads.containsKey(distributionToCompare.getLODVaderID()))
							try {

								// check whether the resource filters of the
								// datasets have already been loaded
								if (!datasetResourceData.containsKey(distributionToCompare.getTopDatasetID())) {
									datasetResourceData.put(distributionToCompare.getTopDatasetID(),
											new DatasetResourcesData(distributionToCompare.getTopDatasetID()));

//									if (datasetResourceData.size() > 100) {
////										for(DatasetDB: datasetResourceData.values())
//										
//										for(DatasetResourcesData dataset: datasetResourceData.values()){
//											System.out.println(dataset.dataset.getTitle());
//											System.out.println("\n");
//										}
										
//										System.out.println("Oppened datasets: " + datasetResourceData.size());
//										System.out.println("Oppened distributions: " + distributionsToCompare.size());
//									}

								}

								// check if distributions had already been
								// compared
								if (!(distributionToCompare.getLODVaderID() == distribution.getLODVaderID())) {
									LinksetDataThread workerThread = new LinksetDataThread(distribution,
											distributionToCompare,
											distributionsResourceData.get(distributionToCompare.getLODVaderID()),
											tuplePart);
									if (workerThread.sourceDatasetID != 0) {
										mapOfWorkerThreads.put(distributionToCompare.getLODVaderID(), workerThread);
										workerThread = mapOfWorkerThreads.get(distributionToCompare.getLODVaderID());
									}
								}
							} catch (Exception e) {
								e.printStackTrace();
								// System.out.println(distributionToCompare);
							}
					}

					for (DistributionResourcesData dNS : distributionsResourceData.values()) {
						// check whether NS is in the subject list
						if (tuplePart.equals(TuplePart.SUBJECT)) {
							for (String ns : localNSCopy) {
								if (dNS.queryObjectNS(ns)) {
									boolean keepTrying = true;
									while (keepTrying) {
										try {
											if (!(dNS.distributionID == distribution.getLODVaderID())) {
												mapOfWorkerThreads.get(dNS.distributionID).active = true;
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
											if (!(dNS.distributionID == distribution.getLODVaderID())) {
												mapOfWorkerThreads.get(dNS.distributionID).active = true;
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

					if (mapOfWorkerThreads.size() > 0) {

						int threadIndex = 0;

						Thread[] threads = new Thread[mapOfWorkerThreads.size()];

						// System.out.println(numberOfOpenThreads);

						// for (Integer in : listOfWorkerThreads.keySet())
						for (LinksetDataThread dataThread : mapOfWorkerThreads.values()) {
							if (dataThread.targetDistributionID != distribution.getLODVaderID())
								if (dataThread.active) {
									if (threads[threadIndex] == null) {
										threads[threadIndex] = new Thread(new LinksetExtractorThread(dataThread,
												(HashMap<String, String>) resourcesToBeProcessedQueueCopy.clone(),
												datasetResourceData));
										threads[threadIndex].setName("MakeLinkSetWorker-" + threadIndex + "-"
												+ dataThread.targetDistributionID);
										threads[threadIndex].start();
										threadIndex++;
									} else {
										// threads[threadIndex].
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
					for (LinksetDataThread dataThread : mapOfWorkerThreads.values()) {
						dataThread.active = false;
					}
				}

			});
			threadNumber++;
			t.setName("MakingLinksets:" + (threadNumber) + ":" + distribution.getUri());
			mapOfThreads.put("MakingLinksets:" + (threadNumber) + ":" + distribution.getUri(), t);
			t.start();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
