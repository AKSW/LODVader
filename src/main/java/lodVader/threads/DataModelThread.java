package lodVader.threads;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import lodVader.TuplePart;
import lodVader.linksets.DistributionFilter;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.gridFS.SuperBucket;

public class DataModelThread extends Thread{

	// true if the source distribution is the subject column
	//
	// sourceColumnIsSubject = true
	//
	// Target Source Target
	// BF dist. BF
	// ____ __________ ____
	// | o| <- | s| p| o| |s |
	// | o| <- | s| p| o| |s |
	// | o| <- | s| p| o| |s |
	// | o| <- | s| p| o| |s |
	// | o| <- | s| p| o| |s |
	//
	// sourceColumnIsSubject = false
	//
	// Target Source Target
	// BF dist. BF
	// ____ __________ ____
	// | o| | s| p| o| -> |s |
	// | o| | s| p| o| -> |s |
	// | o| | s| p| o| -> |s |
	// | o| | s| p| o| -> |s |
	//
	//

	String targetDistributionTitle;

	// public boolean isSubject;

	// 0 for filter not loaded, 1 for loading and 2 for loaded
	public AtomicInteger filterLoaded = new AtomicInteger(0);

	public int distributionID = 0;
	public int datasetID = 0;

	public int targetDistributionID = 0;
	public int targetDatasetID = 0;

	public String filterPath;
	
	public ConcurrentHashMap<String, Integer> validLinks = new ConcurrentHashMap<String, Integer>(); 
	public ConcurrentHashMap<String, Integer> invalidLinks = new ConcurrentHashMap<String, Integer>(); 

	public AtomicInteger numberOfValidLinks = new AtomicInteger(0);
	public AtomicInteger numberOfInvalidLinks = new AtomicInteger(0);
	public int ontologyLinks = 0;

	public ArrayList<? extends SuperBucket> filters = null;

	public HashSet<String> targetNSSet = new HashSet<String>();

	public String tuplePart;

	// flat to execute or not this model in a thread
	public boolean active = true;

	public DataModelThread(DistributionDB sourceDistribution, DistributionDB targetDistribution,
			DistributionFilter distributionFilter, String tuplePart) {
		
		this.tuplePart = tuplePart;
		this.datasetID = sourceDistribution.getTopDataset();
		this.distributionID = sourceDistribution.getLODVaderID();
		this.targetDistributionID = targetDistribution.getLODVaderID();
		this.targetDatasetID = targetDistribution.getTopDataset();
		this.targetDistributionTitle = targetDistribution.getTitle();

		if (tuplePart.equals(TuplePart.SUBJECT)) {
			this.filters = distributionFilter.objectBuckets;
			this.targetNSSet = distributionFilter.objectsNS;
		} else if ((tuplePart.equals(TuplePart.OBJECT))) {
			this.filters = distributionFilter.subjectBuckets;
			this.targetNSSet = distributionFilter.subjectsNS;
		}
	}

}
