package lodVader.threads;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.hp.hpl.jena.sparql.function.library.date;

import lodVader.LODVaderProperties;
import lodVader.Manager;
import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.bloomfilters.models.DatasetLinksContainer;
import lodVader.enumerators.TuplePart;
import lodVader.linksets.DistributionResourcesData;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.gridFS.SuperBucket;

public class LinksetDataThread extends Thread {

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

	public String targetDistributionTitle;


	public int sourceDistributionID = 0;
	public int sourceDatasetID = 0;

	public int targetDistributionID = 0;
	public int targetDatasetID = 0;

	// 0 for filter not loaded, 1 for loading and 2 for loaded
	public AtomicInteger filterLoaded = new AtomicInteger(0);

	private HashMap<String, Integer> validLinks = null;
	private HashMap<String, Integer> invalidLinks = null;

	public BufferedWriter validLinksWriter;
	public BufferedWriter invalidLinksWriter;

	public AtomicInteger numberOfValidLinks = new AtomicInteger(0);
	public AtomicInteger numberOfInvalidLinks = new AtomicInteger(0);
	
	public TreeMap<String, ? extends SuperBucket> distributionFilters = null;

//	public HashSet<String> targetNSSet = new HashSet<String>(); 
	public GoogleBloomFilter targetNSSet;

	public TuplePart tuplePart;

	// flat to execute or not this model in a thread
	public boolean active = false;
	
	DatasetLinksContainer datasetLinkContainer;

	public LinksetDataThread(DistributionDB sourceDistribution, DistributionDB targetDistribution,
			DistributionResourcesData distributionFilter, TuplePart tuplePart) {

		this.tuplePart = tuplePart;
		this.sourceDatasetID = sourceDistribution.getTopDatasetID();
		this.sourceDistributionID = sourceDistribution.getLODVaderID();
		this.targetDistributionID = targetDistribution.getLODVaderID();
		this.targetDatasetID = targetDistribution.getTopDatasetID();
		this.targetDistributionTitle = targetDistribution.getTitle();
		
		this.datasetLinkContainer = new DatasetLinksContainer(this.sourceDistributionID, this.targetDatasetID);
		

		try {
			validLinksWriter = new BufferedWriter(new FileWriter(LODVaderProperties.TMP_FOLDER + "valid_"
					+ this.sourceDistributionID + "_" + this.targetDistributionID));
			invalidLinksWriter = new BufferedWriter(new FileWriter(LODVaderProperties.TMP_FOLDER + "invalid_"
					+ this.sourceDistributionID + "_" + this.targetDistributionID));						

		} catch (IOException e) {
			e.printStackTrace();
		}

		if (tuplePart.equals(TuplePart.SUBJECT)) {
			this.distributionFilters = distributionFilter.objectBuckets;
//			this.targetNSSet = distributionFilter.objectsNS;
			this.targetNSSet = distributionFilter.filterObjectsNS;
		} else if ((tuplePart.equals(TuplePart.OBJECT))) {
			this.distributionFilters = distributionFilter.subjectBuckets;
//			this.targetNSSet = distributionFilter.subjectsNS;
			this.targetNSSet = distributionFilter.filterSubjectsNS;
		}
	}

	public void addValidLink(String resource) {
		if(!datasetLinkContainer.queryDataset(resource)){
			datasetLinkContainer.incrementDatasetCounter();
			datasetLinkContainer.addResource(resource);
		}
		try {
			validLinksWriter.write(resource + "\n");
				
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addInvalidLink(String resource) {
		try {
			invalidLinksWriter.write(resource + "\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setValidLinks(HashMap<String, Integer> l){
		validLinks = l;
	} 

	public void setInvalidLinks(HashMap<String, Integer> l){
		invalidLinks = l;
	}

	public HashMap<String, Integer> getAllValidLinks() {
		try {
			validLinksWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (validLinks == null)
			validLinks = getLinks(LODVaderProperties.TMP_FOLDER + "valid_" + this.sourceDistributionID + "_"
					+ this.targetDistributionID);

		return validLinks;
	}

	public HashMap<String, Integer> getAllInvalidLinks() {
		try {
			invalidLinksWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (invalidLinks == null)
			invalidLinks = getLinks(LODVaderProperties.TMP_FOLDER + "invalid_" + this.sourceDistributionID + "_"
					+ this.targetDistributionID);

		return invalidLinks;
	}

	public HashMap<String, Integer> getLinks(String fileName) {
		HashMap<String, Integer> links = new HashMap<String, Integer>();
		String resource = null;
		BufferedReader br = null;
		Integer n = null;
		try {
			br = new BufferedReader(new FileReader(fileName));
			while ((resource = br.readLine()) != null) {
				n = links.get(resource);
				if (n != null)
					links.put(resource, n + 1);
				else
					links.put(resource, 1);
				if(links.size()>10000000)
					break;
			}

			br.close();

			File f = new File(fileName);
			f.delete();

		} catch (FileNotFoundException e){
			
		}
		
		catch (Exception e) {
			e.printStackTrace();
		}
		return links;
	}

	public void closeFiles() {
		try {
			validLinksWriter.close();
			invalidLinksWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
