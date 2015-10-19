package lodVader.threads;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import lodVader.TuplePart;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.linksets.DistributionNS;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.DistributionObjectNSDB;
import lodVader.mongodb.collections.DistributionSubjectNSDB;
import lodVader.utils.NSUtils;

public class ProcessNSFromTuple extends Thread {
	final static Logger logger = Logger
			.getLogger(ProcessNSFromTuple.class);

//	public boolean isSubject = false;
	
	public String tuplePart;
	
	public int threshold = 50;
	
	// contains all FQDN described by distribution
	// <FQDN, <list of distribution that describes this fqdn>> 
	public ConcurrentHashMap<Integer, DistributionNS>  nsPerDistribution = 
			new ConcurrentHashMap<Integer, DistributionNS>();
	

	protected int threadNumber = 0;
	
	protected ConcurrentHashMap<String, Thread> listOfThreads = new ConcurrentHashMap<String, Thread>();

	private String uri;
	public DistributionDB distributionMongoDBObject = null;
	
	public HashMap<String,Integer> localNS = new HashMap<String,Integer>();

	private boolean doneSplittingString;

	private ConcurrentLinkedQueue<String> resourceQueue = null;
	protected ArrayList<String> resourcesToBeProcessedQueue = new  ArrayList<String>();
	public DistributionDB distribution;
	protected ConcurrentHashMap<Integer, DataModelThread> listOfWorkerThreads = new ConcurrentHashMap<Integer, DataModelThread>(); 
	public ConcurrentHashMap<String, Integer> listLoadedNS = new ConcurrentHashMap<String, Integer>();
	


	private ConcurrentHashMap<String, Integer> countTotalFQDN = null;

	int numberOfReadedTriples = 0;

	int saveDomainsEach = 20000;

	public ProcessNSFromTuple(
			ConcurrentLinkedQueue<String> resourceQueue,
			String uri) {
		this.resourceQueue = resourceQueue;
		this.countTotalFQDN = new ConcurrentHashMap<String, Integer>();
		this.uri = uri;
		this.distribution = new DistributionDB(uri);

	}

	public boolean isDoneSplittingString() {
		return doneSplittingString;
	}

	public void setDoneSplittingString(boolean doneSplittingString) {
		this.doneSplittingString = doneSplittingString;
	}

	public synchronized void run() {

		logger.debug("Starting GetDomainsFromTriplesThread class.");

		NSUtils nsUtils = new NSUtils();
		
		String obj = "";
		while (!doneSplittingString) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			while (resourceQueue.size() > 0) {
				numberOfReadedTriples++;
				try {
					obj = resourceQueue.remove();
					resourcesToBeProcessedQueue.add(obj);
					
					obj = nsUtils.getNSFromString(obj);

					if (!obj.equals("")) {
						countTotalFQDN.putIfAbsent(obj, 0);
						countTotalFQDN.replace(obj, countTotalFQDN.get(obj) + 1);
						localNS.put(obj, 0);
						
					}
					if (numberOfReadedTriples%saveDomainsEach==0){		
						if(tuplePart.equals(TuplePart.OBJECT))	
							makeLinks();
					}

				} catch (NoSuchElementException e) {
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
				
		logger.info("Waiting all threads finish their jobs...");
		try {
			if(tuplePart.equals(TuplePart.OBJECT))
				makeLinks();
			for(Thread t : listOfThreads.values()){
				t.join();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		saveDomains();
		listOfWorkerThreads  = new ConcurrentHashMap<Integer, DataModelThread>(); 
		
		logger.debug("Ending GetDomainsFromTriplesThread class.");
	}

	private boolean saveDomains() {
		logger.debug("Saving domains...");
		ObjectId id = new ObjectId();

		Iterator it = countTotalFQDN.entrySet().iterator();
		
		if(distributionMongoDBObject==null)
			distributionMongoDBObject = new DistributionDB(uri);

		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String d = (String) pair.getKey();
			int count = (Integer) pair.getValue();
			// distributionMongoDBObj.addAuthorityObjects(d);

			if (count > threshold) {
				id = new ObjectId();
				if (tuplePart.equals(TuplePart.SUBJECT)) {
					DistributionSubjectNSDB d2 = new DistributionSubjectNSDB(id.get()
							.toString());
					d2.setSubjectFQDN(d);
					d2.setDistributionID(distributionMongoDBObject.getLODVaderID());
					d2.setNumberOfResources(count);
					
					try {
						d2.updateObject(true);

					} catch (LODVaderLODGeneralException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {

					DistributionObjectNSDB d2 = null;
					d2 = new DistributionObjectNSDB(id.get()
							.toString());
					d2.setObjectFQDN(d);
					d2.setNumberOfResources(count);
					d2.setDistributionID(distributionMongoDBObject.getLODVaderID());

					try {
						d2.updateObject(true);
					} catch (LODVaderLODGeneralException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

			}
		}
		
		return true;
	}
	
public void makeLinks() throws Exception{
		throw new Exception("You have to implement this method."); 
	}

}
