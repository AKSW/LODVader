package lodVader.spring.REST.models.dataset;

import java.util.ArrayList;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.LODVaderProperties;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.mongodb.queries.LinksetQueries;
import lodVader.spring.REST.models.RESTMsg;

public class StatusModel extends RESTMsg {
	
	private ArrayList<DistributionDB> listOfDistributions = new ArrayList<DistributionDB>();

	private ArrayList<DistributionDegree> distributions = new ArrayList<DistributionDegree>();

	final static Logger logger = LoggerFactory.getLogger(StatusModel.class);

	private class DistributionDegree {
		
		public DistributionDegree(DistributionDB distribution, ArrayList<Degree> indegree,
				ArrayList<Degree> outdegree) {
			this.distribution = distribution;
			this.indegree = indegree;
			this.outdegree = outdegree;
		}

		DistributionDB distribution;
		
		ArrayList<Degree> indegree = new ArrayList<Degree>();
		
		ArrayList<Degree> outdegree = new ArrayList<Degree>();

		public DistributionDB getDistribution() {
			return distribution;
		}

		public void setDistribution(DistributionDB distribution) {
			this.distribution = distribution;
		}

		public ArrayList<Degree> getIndegree() {
			return indegree;
		}

		public void setIndegree(ArrayList<Degree> indegree) {
			this.indegree = indegree;
		}

		public ArrayList<Degree> getOutdegree() {
			return outdegree;
		}

		public void setOutdegree(ArrayList<Degree> outdegree) {
			this.outdegree = outdegree;
		}
		
	}
	
	private class Degree {

		DistributionDB distribution;

		double links;

		public Degree(DistributionDB distribution, double links) {

			this.distribution = distribution;

			this.links = links;
		}

		public DistributionDB getDistribution() {
			return distribution;
		}

		public void setDistribution(DistributionDB distribution) {
			this.distribution = distribution;
		}

		public double getLinks() {
			return links;
		}

		public void setLinks(double links) {
			this.links = links;
		}

	}

	public StatusModel(String url) {

		listOfDistributions = new DistributionQueries().getDistributionsByTopDatasetURL(url);

		setCoreMsgSuccess();

		for (DistributionDB distribution : listOfDistributions) {

			ArrayList<DatasetDB> d = distribution.getDefaultDatasetsAsResources();

			Iterator<DatasetDB> i = d.iterator();

			ArrayList<String> parentNames = new ArrayList<String>();
			while (i.hasNext()) {
				parentNames.add(i.next().getUri());
			}


			ArrayList<Degree> indegreeList = new ArrayList<Degree>();
			// indegrees
			ArrayList<LinksetDB> indegrees = new LinksetQueries().getLinksetsInDegreeByDistribution(
					distribution.getLODVaderID(), LinksetDB.LINK_NUMBER_LINKS, LODVaderProperties.LINKSET_TRESHOLD, -1);
			int indegreeCount = 0;
			
			for (LinksetDB linkset : indegrees) {
				
				indegreeList.add(new Degree(new DistributionDB(linkset.getDistributionSource()), linkset.getLinks()));
				
			}

			// outdegrees
			ArrayList<Degree> outdegreeList = new ArrayList<Degree>();

			ArrayList<LinksetDB> outdegrees = new LinksetQueries().getLinksetsOutDegreeByDistribution(
					distribution.getLODVaderID(), LinksetDB.LINK_NUMBER_LINKS, LODVaderProperties.LINKSET_TRESHOLD, -1);
			// int outdegreeCount = 0;
			// JSONArray outdegreeArray = new JSONArray();
			for (LinksetDB linkset : outdegrees) {
				
				outdegreeList.add(new Degree(new DistributionDB(linkset.getDistributionTarget()), linkset.getLinks()));

			}
			
			distributions.add(new DistributionDegree(distribution, indegreeList, outdegreeList));

		}
	}

	public ArrayList<DistributionDegree> getDistributions() {
		return distributions;
	}

	public void setDistributions(ArrayList<DistributionDegree> distributions) {
		this.distributions = distributions;
	}
	
	
}
