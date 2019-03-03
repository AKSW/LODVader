package lodVader.spring.REST.models.distribution;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import lodVader.ServiceAPIOptions;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.toplinks.TopInvalidLinks;
import lodVader.mongodb.collections.toplinks.TopValidLinks;
import lodVader.mongodb.queries.TopNLinksQueries;

public class CompareTopNModel {

	private ArrayList<TopN> topNLinks = new ArrayList<TopN>();
	
	private DistributionDB distribution1;

	private DistributionDB distribution2;
	
	private String type;
	

	private class TopN {

		String url;

		Double amount;

		public TopN(String url, Double amount) {
			this.url = url;
			this.amount = amount;
		}

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		public Double getAmount() {
			return amount;
		}

		public void setAmount(Double amount) {
			this.amount = amount;
		}
	}

	public void getTopNLinks(int distribution1ID, int distribution2ID, String type) {
		
		distribution1 = new DistributionDB(distribution1ID);
		distribution2 = new DistributionDB(distribution2ID);
		this.type=type;
		
		String collectionName;
		if (type.equals(ServiceAPIOptions.DATASET_TYPE_LINKS))
			collectionName = TopValidLinks.COLLECTION_NAME;
		else
			collectionName = TopInvalidLinks.COLLECTION_NAME;

		LinkedHashMap<String, Integer> links = new TopNLinksQueries().getTopNLinks(distribution1ID, distribution2ID,
				collectionName);

		for (String url : links.keySet()) {
			topNLinks.add(new TopN(url, Double.valueOf(links.get(url))));
		}
	}

	public ArrayList<TopN> getTopNLinks() {
		return topNLinks;
	}

	public void setTopNLinks(ArrayList<TopN> topNLinks) {
		this.topNLinks = topNLinks;
	}

	public DistributionDB getDistribution1() {
		return distribution1;
	}

	public void setDistribution1(DistributionDB distribution1) {
		this.distribution1 = distribution1;
	}

	public DistributionDB getDistribution2() {
		return distribution2;
	}

	public void setDistribution2(DistributionDB distribution2) {
		this.distribution2 = distribution2;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	
}
