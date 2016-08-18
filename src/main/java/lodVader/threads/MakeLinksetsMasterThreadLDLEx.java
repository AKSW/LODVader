package lodVader.threads;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ldlex.seeder.MapperService;
import lodVader.LODVaderProperties;
import lodVader.enumerators.TuplePart;
import lodVader.mongodb.collections.DistributionDB;

public class MakeLinksetsMasterThreadLDLEx extends ProcessNSFromTupleLDLEX {

	/**
	 * Create linksets for a distribution
	 * 
	 * @param resourceQueue
	 *            string queue of objects or subjects resources
	 * @param uri
	 *            of the distribution (usually the distribution URL)
	 * @throws MalformedURLException
	 */
	public MakeLinksetsMasterThreadLDLEx(BlockingQueue<String> resourceQueue, String uri, TuplePart tuplePart)
			throws MalformedURLException {
		super(resourceQueue, uri,tuplePart);
	}

	final static Logger logger = LoggerFactory.getLogger(MakeLinksetsMasterThreadLDLEx.class);

	ArrayList<DistributionDB> distributionsToCompare;

	public HashSet<String> localNS0Copy;
	public HashSet<String> localNSCopy;

	@Override
	public void makeLinks(final int treshold) {

		localNSCopy = chunkOfNS;
		localNS0Copy = chunkOfNS0;
		final HashMap<String, String> resourcesToBeProcessedQueueCopy = resourcesToBeProcessedBuffer;

		// localNS = new HashSet<String>();

		chunkOfNS0 = new HashSet<String>();
		chunkOfNS = new HashSet<String>();
		resourcesToBeProcessedBuffer = new HashMap<String, String>();

		try {
			Thread t = new Thread(new Runnable() {
				public void run() {


					numberThreadsWaiting.incrementAndGet();
					while (numberOfMakeLinksActiveThreads.get() >= 1) {

						try {
							Thread.sleep(80);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					numberThreadsWaiting.decrementAndGet();

					numberOfMakeLinksActiveThreads.incrementAndGet();

					// System.out.println("Starting making links.");

					ConcurrentHashMap<String, HashSet<String>> nsLists = new ConcurrentHashMap<>();
					ArrayList<String> nsToSearch = new ArrayList<String>();

					// create a list of NS which should be fetched from
					// database
					// and add the loaded NS to a global map
					for (String ns0 : localNS0Copy) {
						if (!mapper.hasNS(ns0)) {
							nsToSearch.add(ns0);
							LoadedNSCounter.put(ns0, 0);
						}
					}

					// Update namespace - distribution map
					if (nsToSearch.size() > 0)
						new MapperService().updateMapping(nsToSearch, tuplePart, mapper);

					for (String resource : resourcesToBeProcessedQueueCopy.keySet()) {
						String ns = resourcesToBeProcessedQueueCopy.get(resource);

						// add resource to namespace list
						HashSet<String> nsHash = nsLists.get(ns);
						if (nsHash == null) {
							nsHash = new HashSet<>();
							nsHash.add(resource);
							nsLists.put(ns, nsHash);
						} else {
							nsHash.add(resource);
						}
					}

					// load distributions

					for (String ns : nsLists.keySet())
						for (Integer targetDistributionID : mapper.getDistributions(ns)) {
							try {

								// if the distribution has not been loaded
								if (distributionStatus.get(targetDistributionID) == null) {
									DistributionDB targetDistribution = new DistributionDB(targetDistributionID);
									distributionStatus.put(targetDistributionID, 1);
									mapOfWorkerThreads.put(targetDistributionID,
											new LinksetDataThreadLDLEx(targetDistribution, tuplePart));

									mapOfWorkerThreads.get(targetDistributionID).resources.put(ns,nsLists.get(ns)); 
									distributionStatus.put(targetDistributionID, 2);

								} else if (distributionStatus.get(targetDistributionID) == 1) {

									while (distributionStatus.get(targetDistributionID) == 1) {
										try {
											Thread.sleep(30);
										} catch (InterruptedException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
									}

								} else if (distributionStatus.get(targetDistributionID) == 2) {
									// add resource to the correct distribution
									mapOfWorkerThreads.get(targetDistributionID).resources.put(ns,nsLists.get(ns));
//									mapOfWorkerThreads.get(targetDistributionID).resources.add(resource);

									// while
									// (mapOfWorkerThreads.get(targetDistributionID).resources.size()
									// > 150000)
									// try {
									// logger.info(String.valueOf(mapOfWorkerThreads.get(targetDistributionID).resources.size()));
									// Thread.sleep(30);
									// } catch (InterruptedException e) {
									// // TODO Auto-generated catch block
									// e.printStackTrace();
									// }

								}

							} catch (NullPointerException e) {
								e.printStackTrace();
								distributionStatus.put(targetDistributionID, null);

							}

						}

					// distribute the resources to be processed to the correct
					// distributions
					// for (String resource :
					// resourcesToBeProcessedQueueCopy.keySet()) {
					// String ns =
					// resourcesToBeProcessedQueueCopy.get(resource);

					// for (Integer targetDistributionID :
					// mapper.getDistributions(ns)) {
					//
					// try {
					//
					// // if the distribution has not been loaded
					// if (distributionStatus.get(targetDistributionID) == null)
					// {
					// DistributionDB targetDistribution = new
					// DistributionDB(targetDistributionID);
					// distributionStatus.put(targetDistributionID, 1);
					// mapOfWorkerThreads.put(targetDistributionID,
					// new LinksetDataThreadLDLEx(targetDistribution,
					// tuplePart));
					//
					// distributionStatus.put(targetDistributionID, 2);
					// mapOfWorkerThreads.get(targetDistributionID).resources.add(resource);
					//
					// } else if (distributionStatus.get(targetDistributionID)
					// == 1) {
					//
					// while (distributionStatus.get(targetDistributionID) == 1)
					// {
					// try {
					// Thread.sleep(30);
					// } catch (InterruptedException e) {
					// // TODO Auto-generated catch block
					// e.printStackTrace();
					// }
					// }
					//
					// } else if (distributionStatus.get(targetDistributionID)
					// == 2) {
					// // add resource to the correct distribution
					// mapOfWorkerThreads.get(targetDistributionID).resources.add(resource);
					//
					// // while
					// //
					// (mapOfWorkerThreads.get(targetDistributionID).resources.size()
					// // > 150000)
					// // try {
					// //
					// logger.info(String.valueOf(mapOfWorkerThreads.get(targetDistributionID).resources.size()));
					// // Thread.sleep(30);
					// // } catch (InterruptedException e) {
					// // // TODO Auto-generated catch block
					// // e.printStackTrace();
					// // }
					//
					// }
					//
					// } catch (NullPointerException e) {
					// e.printStackTrace();
					// distributionStatus.put(targetDistributionID, null);
					//
					// }
					//
					// }
					// }

					// int bufferSize = resourcesToBeProcessedQueueCopy.size();

					// String[] buffer = new String[bufferSize];

					if (mapOfWorkerThreads.size() > 0) {
						
						ExecutorService executor = Executors.newFixedThreadPool(4);

						int threadIndex = 0;

						Thread[] threads = new Thread[mapOfWorkerThreads.size()];

						for (LinksetDataThreadLDLEx dataThread : mapOfWorkerThreads.values()) {
							if (distribution.getLODVaderID() != dataThread.distributionID)
								if (dataThread.resources.size() > treshold && !dataThread.isBeingConsumed.get()) {
									if (threads[threadIndex] == null) {
										dataThread.isBeingConsumed.set(true);
										threads[threadIndex] = new Thread(
												new LinksetExtractorThreadLDLEx(dataThread, dataThread.resources));
										threads[threadIndex]
												.setName("worker--" + dataThread.distribution.getDownloadUrl());

//										while (numberOfWorkerActiveThreads.get() >= LODVaderProperties.NR_THREADS)
//											try {
//												Thread.sleep(6);
//											} catch (InterruptedException e) {
//												e.printStackTrace();
//											}
										numberOfWorkerActiveThreads.incrementAndGet();
//										threads[threadIndex].start();
										executor.submit(threads[threadIndex]);
										threadIndex++;
									} else {
										// threads[threadIndex].
									}
								}
						}

						executor.shutdown();
						try {
							executor.awaitTermination(1, TimeUnit.DAYS);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						// wait all threads finish
//						for (int d = 0; d < threads.length; d++)
//							if (threads[d] != null)
//								try {
//									threads[d].join();
//								} catch (Exception e) {
//									// TODO Auto-generated catch block
//									e.printStackTrace();
//								}
					}

					// save linksets into mongodb
					// for (LinksetDataThreadLDLEx dataThread :
					// mapOfWorkerThreads.values()) {
					// dataThread.resources = new HashSet<String>();
					// }
					numberOfMakeLinksActiveThreads.decrementAndGet();
					logger.info("MakeLink thread finished. " + tuplePart.toString() + " resources processed: "
							+ numberOfFinishedThreads.incrementAndGet() * LODVaderProperties.CHECK_LINKS_EACH
							+ ". Threads waiting: " + numberThreadsWaiting.get() + ". Queue size: "
							+ resourceQueue.size());

				}

			});
			threadNumber++;
			t.setName("MakingLinksets:" + (threadNumber) + ":" + distribution.getUri());
			mapOfThreads.put("MakingLinksets:" + (threadNumber) + ":" + distribution.getUri(), t);
			
//			while (numberThreadsWaiting.get() >= 50) {
//
//				try {
//					Thread.sleep(5000);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}

			t.start();

		} catch (

		Exception e) {
			e.printStackTrace();
		}
	}

}
