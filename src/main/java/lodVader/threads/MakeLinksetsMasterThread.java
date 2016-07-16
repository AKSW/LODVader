package lodVader.threads;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.LODVaderProperties;
import lodVader.enumerators.TuplePart;
import lodVader.linksets.DatasetBloomFilterContainer;
import lodVader.linksets.DistributionBloomFilterContainer;
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

	public HashSet<String> localNS0Copy;
	public HashSet<String> localNSCopy;

	@Override
	public void makeLinks() {

		localNSCopy = chunkOfNS;
		localNS0Copy = chunkOfNS0;
		final HashMap<String, String> resourcesToBeProcessedQueueCopy = resourcesToBeProcessed;
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

					// get which distributions describe which NS and save in
					// a
					// list (so we don't have to query again)
					if (tuplePart.equals(TuplePart.OBJECT)) {
						logger.info("Loading subjects NS from MongoDB.");
						Timer t = new Timer();
						t.startTimer();
						distributionsToCompare = new DistributionQueries().getDistributionsByOutdegree(nsToSearch,
								distributionsResourceData);
						logger.info("Done loading subjects NS from MongoDB. Time to fetch: " + t.stopTimer());
					}

					else if (tuplePart.equals(TuplePart.SUBJECT)) {
						logger.info("Loading objects NS from MongoDB.");
						Timer t = new Timer();
						t.startTimer();
						distributionsToCompare = new DistributionQueries().getDistributionsByIndegree(nsToSearch,
								distributionsResourceData);
						logger.info("Done loading objects NS from MongoDB. Time to fetch: " + t.stopTimer());
					}

					for (DistributionDB distributionToCompare : distributionsToCompare) {

						if (!mapOfWorkerThreads.containsKey(distributionToCompare.getLODVaderID()))
							try {

								// check whether the resource filters of the
								// datasets have already been loaded
								// if
								// (!datasetResourceData.containsKey(distributionToCompare.getTopDatasetID()))
								// {
								// datasetResourceData.put(distributionToCompare.getTopDatasetID(),
								// new
								// DatasetBloomFilterContainer(distributionToCompare.getTopDatasetID()));

								// if (datasetResourceData.size() > 100) {
								//// for(DatasetDB:
								// datasetResourceData.values())
								//
								// for(DatasetResourcesData dataset:
								// datasetResourceData.values()){
								// System.out.println(dataset.dataset.getTitle());
								// System.out.println("\n");
								// }

								// System.out.println("Opened datasets: " +
								// datasetResourceData.size());
								// System.out.println("Opened
								// distributions: " +
								// distributionsToCompare.size());
								// }

								// }

								// check if distributions had already been
								// compared

								// System.out.println(distributionToCompare.getDownloadUrl());

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

					for (DistributionBloomFilterContainer dNS : distributionsResourceData.values()) {
						// check whether NS is in the subject list
						if (tuplePart.equals(TuplePart.SUBJECT)) {
							for (String ns : localNSCopy) {
								if (dNS.queryObjectNS(ns)) {
									boolean keepTrying = true;
									while (keepTrying) {
										try {
											if (!(dNS.getDistributionID() == distribution.getLODVaderID())) {
												mapOfWorkerThreads.get(dNS.getDistributionID()).active = true;
											}
											keepTrying = false;
										} catch (Exception e) {
											// e.printStackTrace();
											try {
												// sleep here while the BF
												// are not loaded yet (they
												// are being loaded by a
												// previous thread)
												Thread.sleep(100);
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
											if (!(dNS.getDistributionID() == distribution.getLODVaderID())) {
												mapOfWorkerThreads.get(dNS.getDistributionID()).active = true;
											}

											keepTrying = false;
										} catch (Exception e) {
											// e.printStackTrace();
											try {
												// sleep here while the BF
												// are not loaded yet (they
												// are being loaded by a
												// previous thread)
												Thread.sleep(100);
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
												resourcesToBeProcessedQueueCopy, datasetResourceData));
										threads[threadIndex].setName("MakeLinkSetWorker-" + threadIndex + "-"
												+ dataThread.targetDistributionID);
										
										while(numberOfActiveThreads.get() >= LODVaderProperties.NR_THREADS)
											try {
												Thread.sleep(10);
											} catch (InterruptedException e) {
												// TODO Auto-generated catch block
												e.printStackTrace();
											}
										numberOfActiveThreads.incrementAndGet();
										threads[threadIndex].start();
										threadIndex++;
									} else {
										// threads[threadIndex].
									}
								}
						}

						// wait all threads finish
						for (int d = 0; d < threads.length; d++)
							if (threads[d] != null)
								try {
									threads[d].join();
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
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
