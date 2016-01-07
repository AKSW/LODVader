package lodVader.spring.REST.models.predicate;

import java.util.HashMap;
import java.util.Set;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import lodVader.mongodb.queries.PredicatesQueries;
import lodVader.utils.NSUtils;

public class ListModel {

	StringBuilder csv = new StringBuilder();

	public ListModel(String predicateSearch) {

		PredicatesQueries q = new PredicatesQueries();
		HashMap<Integer, HashMap<String, Integer>> distributions;

		if (!predicateSearch.equals("")) {
			Set<AllPredicatesDB> ids = q.getAllPredicatesRegex(predicateSearch);

			// get datasets
			distributions = q.getDistributions(ids);
		}
		else
			distributions = q.getDistributions(null);

		makeCSVHeader();

		DistributionDB dist;
		for (int distributionID : distributions.keySet()) {

			dist = new DistributionDB(distributionID);
			String topDatasetTitle = dist.getTopDatasetTitle();
			String downloadURL = dist.getDownloadUrl();
			String triples = dist.getTriples().toString();
			

			HashMap<String, Integer> predicates = distributions.get(distributionID);

			for (String predicate : predicates.keySet()) {
				
				if(predicateSearch.equals(""))
					addCSVFieldString(new NSUtils().getNSFromString(predicate));
				else
					addCSVFieldString(predicateSearch);
				addCSVFieldString(predicate);
				addCSVFieldString(topDatasetTitle);
				addCSVFieldString(downloadURL);
				addCSVFieldInt(triples);
				addCSVLastField(predicates.get(predicate).toString());
				addCSVLine();
			}
		}

	}

	private void makeCSVHeader() {
		csv.append("Ontology OR namespace, Ontology Class / Property URI, Dataset Name, distribution, Total nr triples, Nr of triples with Class/Property \n");
	}

	private void addCSVFieldString(String field) {
		csv.append("\""+field + "\",");
	}
	
	private void addCSVFieldInt(String field) {
		csv.append(field + ",");
	}
	
	private void addCSVLastField(String field) {
		csv.append(field);
	}
 
	private void addCSVLine() {
		csv.append("\n");
	}

	public String getCsv() {
		return csv.toString();
	}

}
