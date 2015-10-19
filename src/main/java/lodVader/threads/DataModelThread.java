package lodVader.threads;

import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import lodVader.TuplePart;
import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.linksets.DistributionNS;
import lodVader.mongodb.collections.DistributionDB;

public class DataModelThread {

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
	
	String distributionTitle; 

//	public boolean isSubject;

	// 0 for filter not loaded, 1 for loading and 2 for loaded
	public AtomicInteger filterLoaded = new AtomicInteger(0);

	public int distributionID = 0;
	public int datasetID= 0;

	public int targetDistributionID= 0;
	public int targetDatasetID= 0;
	
	public String filterPath;

	public AtomicInteger links = new AtomicInteger(0);
	public AtomicInteger invalidLinks = new AtomicInteger(0);
	public int ontologyLinks = 0;

	public GoogleBloomFilter filter = new GoogleBloomFilter();
	
	public TreeSet<String> targetFQDNTree = new TreeSet<String>();
	
	public String tuplePart;

	// flat to execute or not this model in a thread
	public boolean active = true;

	public DataModelThread(
			DistributionDB distribution,
			DistributionDB targetDistribution, 
			DistributionNS distributionFQDN,
			String tuplePart) {

		
//		DataModelThread dataThread = new DataModelThread();
//		this.isSubject = isSubject;
//		dataThread.describedFQDN = describedFQDN;
		this.tuplePart = tuplePart;
		
		distributionTitle = targetDistribution.getTitle();

		if (!targetDistribution.getUri().equals(distribution.getUri())) {
			// save dataThread object

			if (tuplePart.equals(TuplePart.SUBJECT)){
				this.filterPath = targetDistribution
						.getObjectFilterPath();
				targetFQDNTree = distributionFQDN.objectsFQDN;
			}
			else if((tuplePart.equals(TuplePart.OBJECT))){
				this.filterPath = targetDistribution
						.getSubjectFilterPath();
				targetFQDNTree = distributionFQDN.subjectsFQDN;
			}

			
			this.targetDistributionID = targetDistribution
					.getLODVaderID();
			this.targetDatasetID = targetDistribution.getTopDataset();

			this.datasetID = distribution.getTopDataset();
			this.distributionID = distribution.getLODVaderID();
			// dataThread.distributionObjectPath = distribution
			// .getObjectPath(); 

		}

	}
	
	public void startFilter(){
		while(filterLoaded.get() == 1)
			try {
				Thread.sleep(2);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		
		if(filterLoaded.get()==2) return;
		
		try {
			
			this.filterLoaded.set(1);
			this.filter.loadFilter(filterPath, distributionTitle);
			
			this.filterLoaded.set(2);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
