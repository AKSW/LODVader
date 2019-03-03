package lodVader.spring.REST.models.distribution;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;

import lodVader.ServiceAPIOptions;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.queries.LinksetQueries;

public class CompareListModel {

	private ArrayList<DistributionList> distributions = new ArrayList<DistributionList>();

	private DistributionDB distribution;

	private class DistributionList {

		private String numberOfLinks;

		private DistributionDB distribution;
 
		public DistributionDB getDistribution() {
			return distribution;
		}

		public void setDistribution(DistributionDB distribution) {
			this.distribution = distribution;
		}

		public String getNumberOfLinks() {
			return numberOfLinks;
		}

		public void setNumberOfLinks(String numberOfLinks) {
			this.numberOfLinks = numberOfLinks;
		}
	}

	public void makeCompareList(int distributionID, int topN, String type) {

		NumberFormat formatterLinks = new DecimalFormat("###,###,###,###");
		NumberFormat formatterDecimal = new DecimalFormat("#.####");

		distribution = new DistributionDB(distributionID);

		ArrayList<LinksetDB> links = new LinksetQueries().getLinksets(distributionID, topN, type);

		for (LinksetDB link : links) {

			DistributionDB distributionTarget = new DistributionDB(link.getDistributionTarget());

			if (distributionID != distributionTarget.getLODVaderID()) {

				DistributionList line = new DistributionList();

				if (type.equals(ServiceAPIOptions.DATASET_TYPE_LINKS))
					line.setNumberOfLinks(formatterLinks.format(link.getLinks()));
				else if (type.equals(ServiceAPIOptions.DATASET_TYPE_TOP_BAD_LINKS))
					if (link.getInvalidLinks() > 10000000)
						line.setNumberOfLinks(">10,000,000");
					else
						line.setNumberOfLinks(formatterLinks.format(link.getInvalidLinks()));
				else if (type.equals(ServiceAPIOptions.DATASET_TYPE_STRENGTH)) 
					line.setNumberOfLinks(formatterDecimal.format(link.getStrength()));
				else if (type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES))
					line.setNumberOfLinks(formatterDecimal.format(link.getOwlClassSimilarity()));
				else if (type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES))
					line.setNumberOfLinks(formatterDecimal.format(link.getRdfSubClassSimilarity()));
				else if (type.equals(ServiceAPIOptions.DATASET_TYPE_RDF_TYPE))
					line.setNumberOfLinks(formatterDecimal.format(link.getRdfTypeSimilarity()));
				else
					line.setNumberOfLinks(formatterDecimal.format(link.getPredicateSimilarity()));

				line.setDistribution(distributionTarget);

				if (!line.getNumberOfLinks().equals("0")) {
					distributions.add(line);
				}
			}
		}
	}

	public ArrayList<DistributionList> getDistributions() {
		return distributions;
	}

	public void setDistributions(ArrayList<DistributionList> distributions) {
		this.distributions = distributions;
	}

	public DistributionDB getDistribution() {
		return distribution;
	}

	public void setDistribution(DistributionDB distribution) {
		this.distribution = distribution;
	}

}
