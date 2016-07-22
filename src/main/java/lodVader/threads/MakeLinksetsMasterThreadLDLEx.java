package lodVader.threads;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ldlex.seeder.MapperService;
import lodVader.LODVaderProperties;
import lodVader.enumerators.TuplePart;
import lodVader.linksets.DistributionBloomFilterContainer;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.utils.Timer;

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
	public MakeLinksetsMasterThreadLDLEx(ConcurrentLinkedQueue<String> resourceQueue, String uri)
			throws MalformedURLException {
		super(resourceQueue, uri);
	}

	final static Logger logger = LoggerFactory.getLogger(MakeLinksetsMasterThreadLDLEx.class);

	ArrayList<DistributionDB> distributionsToCompare;

	public HashSet<String> localNS0Copy;
	public HashSet<String> localNSCopy;

	@Override
	public void makeLinks() {

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
					if(nsToSearch.size()>0)
						new MapperService().updateMapping(nsToSearch, tuplePart, mapper);

					// distribute the resources to be processed to the correct
					// distributions
					for (String resource : resourcesToBeProcessedQueueCopy.keySet()) {
						String ns = resourcesToBeProcessedQueueCopy.get(resource);
 
						for (Integer targetDistributionID : mapper.getDistributions(ns)) {


							// if the distribution has not been loaded
							if (distributionStatus.get(targetDistributionID) == null) {
								DistributionDB targetDistribution = new DistributionDB(targetDistributionID);
								distributionStatus.put(targetDistributionID, 1);
								mapOfWorkerThreads.put(targetDistributionID,
										new LinksetDataThreadLDLEx(targetDistribution, tuplePart));
								
								distributionStatus.put(targetDistributionID, 2);
								mapOfWorkerThreads.get(targetDistributionID).resources.add(resource);

							} else if (distributionStatus.get(targetDistributionID) == 1) {

								while (distributionStatus.get(targetDistributionID) == 1) {
									try {
										Thread.sleep(100);
									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}

							} else if (distributionStatus.get(targetDistributionID) == 2) {
								// add resource to the correct distribution
								mapOfWorkerThreads.get(targetDistributionID).resources.add(resource); 
							}
						}
					}

//					int bufferSize = resourcesToBeProcessedQueueCopy.size();

//					String[] buffer = new String[bufferSize];

					if (mapOfWorkerThreads.size() > 0) {

						int threadIndex = 0;

						Thread[] threads = new Thread[mapOfWorkerThreads.size()];

						for (LinksetDataThreadLDLEx dataThread : mapOfWorkerThreads.values()) {
							if (distribution.getLODVaderID() != dataThread.distributionID)
								if (dataThread.resources.size() > 1000 && !dataThread.isBeingConsumed.get()) {
									if (threads[threadIndex] == null) {
										dataThread.isBeingConsumed.set(true);
										threads[threadIndex] = new Thread(
												new LinksetExtractorThreadLDLEx(dataThread, dataThread.resources)); 
										threads[threadIndex].setName("worker--"
												+ dataThread.distribution.getDownloadUrl());

										while (numberOfActiveThreads.get() >= LODVaderProperties.NR_THREADS)
											try {
												Thread.sleep(20);
											} catch (InterruptedException e) {
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
//					for (LinksetDataThreadLDLEx dataThread : mapOfWorkerThreads.values()) {
//						dataThread.resources = new HashSet<String>();
//					}

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
