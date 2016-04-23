package lodVader.spring.REST.models;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lodVader.LODVaderProperties;
import lodVader.enumerators.TuplePart;
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
	
	public void search(int skip, int limit, Boolean searchVocabularies, String searchNameOrURL,
			String searchSubject, String searchProperty, String searchObject, String searchStatus) {
		
		boolean hasResource = false;
		boolean resourceFound = false;

		if ((!searchSubject.equals("") || !searchProperty.equals("") || !searchObject.equals(""))){
			hasResource = true;
		}

		List<Set<Integer>> setOfSetsOfDistributions = new ArrayList<Set<Integer>>();

		if (!searchSubject.equals("")) {
			HashSet<Integer> i = new HashSet<Integer>();
			for (DistributionDB n : new DistributionQueries().queryDistribution(searchSubject,
					TuplePart.SUBJECT)) {
				i.add(n.getLODVaderID());
			}
			setOfSetsOfDistributions.add(i);
			if (i.size() > 0){
				resourceFound = true;
			}	
		}
		
		if (!searchObject.equals("")) {
			HashSet<Integer> i = new HashSet<Integer>();
			for (DistributionDB n : new DistributionQueries().queryDistribution(searchObject,
					TuplePart.OBJECT)) {
				i.add(n.getLODVaderID());
			}
			setOfSetsOfDistributions.add(i);
			if (i.size() > 0){
				resourceFound = true;
			}
		}
		
		if (!searchProperty.equals("")) {
			HashSet<Integer> i = new HashSet<Integer>();
			for (DistributionDB n : new DistributionQueries().queryDistribution(searchProperty,
					TuplePart.PROPERTY)) {
				i.add(n.getLODVaderID());
			}
			setOfSetsOfDistributions.add(i);
			if (i.size() > 0){
				resourceFound = true;
			}
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
			return;
		}
		
		// search by name
		DistributionQueries dq = new DistributionQueries();
		
		ArrayList<DistributionDB> distributions = dq.getDistributions(skip, limit, searchVocabularies, searchNameOrURL, in,
				searchStatus);
		totalSize = dq.distributionQuerySize;

		this.distributions= distributions; 
	}
	
}
