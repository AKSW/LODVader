package lodVader.spring.REST.models;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lodVader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.DistributionQueries;

public class StatusPageModel {

	ArrayList<DistributionDB> distributions;
	
	int totalSize = 0;

	public ArrayList<DistributionDB> getDistributions() {
		return distributions;
	}

	public void setDistributions(ArrayList<DistributionDB> distributions) {
		this.distributions = distributions;
	}

	public int getTotalSize() {
		return totalSize;
	}

	public void setTotalSize(int totalSize) {
		this.totalSize = totalSize;
	}
	
	public void search(int skip, int limit, Boolean searchVocabularies, String search,
			String searchSubject, String searchProperty, String searchObject, String searchStatus) {

		boolean hasResource = false;
		boolean resourceFound = false;

		if (((searchSubject != "") || (searchProperty != "") || (searchObject != "")))
			hasResource = true;

		List<Set<Integer>> setOfSetsOfDistributions = new ArrayList<Set<Integer>>();

		if (searchSubject != "") {
			HashSet<Integer> i = new HashSet<Integer>();
			for (DistributionDB n : new DistributionQueries().queryDistribution(searchSubject,
					LODVaderProperties.TYPE_SUBJECT)) {
				i.add(n.getLODVaderID());
			}
			setOfSetsOfDistributions.add(i);
			if (i.size() > 0){
				resourceFound = true;
			}
			
		}
		if (searchObject != "") {
			HashSet<Integer> i = new HashSet<Integer>();
			for (DistributionDB n : new DistributionQueries().queryDistribution(searchObject,
					LODVaderProperties.TYPE_OBJECT)) {
				i.add(n.getLODVaderID());
			}
			setOfSetsOfDistributions.add(i);
			if (i.size() > 0)
				resourceFound = true;
		}
		if (searchProperty != "") {
			HashSet<Integer> i = new HashSet<Integer>();
			for (DistributionDB n : new DistributionQueries().queryDistribution(searchProperty,
					LODVaderProperties.TYPE_PROPERTY)) {
				i.add(n.getLODVaderID());
			}
			setOfSetsOfDistributions.add(i);
			if (i.size() > 0)
				resourceFound = true;
		}

		List<Integer> in = new ArrayList<Integer>();

		// get elements in common among the sets
		if (setOfSetsOfDistributions.size() > 0) {
			Set<Integer> setCross = setOfSetsOfDistributions.get(0);
			for (int i = 1; i < setOfSetsOfDistributions.size(); i++) {
				setCross.retainAll(setOfSetsOfDistributions.get(i));
			}
			for (Integer v : setCross) {
				in.add(v);
			}
		}

		if ((!resourceFound && hasResource) || (hasResource && in.size() == 0)) {
			this.distributions = new ArrayList<DistributionDB>();
		}
		
		// search by name
		DistributionQueries dq = new DistributionQueries();
		
		ArrayList<DistributionDB> distributions = dq.getDistributions(skip, limit, searchVocabularies, search, in,
				searchStatus);
		totalSize = dq.distributionQuerySize;

		this.distributions= distributions; 
	}
	
}
