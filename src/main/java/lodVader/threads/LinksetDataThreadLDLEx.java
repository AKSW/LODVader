package lodVader.threads;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import lodVader.LODVaderProperties;
import lodVader.enumerators.TuplePart;
import lodVader.linksets.DistributionBloomFilterContainer;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.gridFS.SuperBucket;

public class LinksetDataThreadLDLEx extends Thread {

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

	public int distributionID = 0;
	public int datasetID = 0;
	public DistributionDB distribution = null;

	// 0 for filter not loaded, 1 for loading and 2 for loaded
	public AtomicInteger filterLoaded = new AtomicInteger(0);
	public AtomicBoolean isBeingConsumed = new AtomicBoolean(false);
	private HashMap<String, Integer> validLinks = null;

	public BufferedWriter validLinksWriter;
	public BufferedWriter invalidLinksWriter;

	public AtomicInteger numberOfValidLinks = new AtomicInteger(0);

	public TreeMap<String, ? extends SuperBucket> distributionFilters = null;

	// public BloomFilterI targetNSSet;
	// HashSet<String> resources = new HashSet<String>();
//	ConcurrentLinkedQueue<String> resources = new ConcurrentLinkedQueue<String>();
	Queue<String> resources = new ConcurrentLinkedQueue<>();

	// flat to execute or not this model in a thread
	public boolean active = false;

	TuplePart tuplePart;

	public LinksetDataThreadLDLEx(DistributionDB distribution, TuplePart tuplePart) {

		this.distribution = distribution;
		this.distributionID = distribution.getLODVaderID();
		this.tuplePart = tuplePart;
		this.datasetID = distribution.getTopDatasetID();
		DistributionBloomFilterContainer distributionFilter = new DistributionBloomFilterContainer(
				distribution.getLODVaderID());

		try {
			validLinksWriter = new BufferedWriter(
					new FileWriter(LODVaderProperties.TMP_FOLDER + "valid_" + this.distributionID));
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (tuplePart.equals(TuplePart.SUBJECT)) {
			distributionFilter.loadObjectBuckets();
			this.distributionFilters = distributionFilter.getObjectBuckets();
			distributionFilter.loadObjectNamespaces();
			// this.targetNSSet = distributionFilter.getFilterObjectsNS();
		} else if ((tuplePart.equals(TuplePart.OBJECT))) {
			distributionFilter.loadSubjectBuckets();
			this.distributionFilters = distributionFilter.getSubjectBuckets();
			distributionFilter.loadSubjectNamespaces();
			// this.targetNSSet = distributionFilter.getFilterSubjectsNS();
		}
	}

	public void addValidLink(String resource) {
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

	public void setValidLinks(HashMap<String, Integer> l) {
		validLinks = l;
	}

	public HashMap<String, Integer> getAllValidLinks() {
		try {
			validLinksWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (validLinks == null)
			validLinks = getLinks(LODVaderProperties.TMP_FOLDER + "valid_" + this.distributionID);

		return validLinks;
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
				if (links.size() > 10000000)
					break;
			}

			br.close();

			File f = new File(fileName);
			f.delete();

		} catch (FileNotFoundException e) {

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
