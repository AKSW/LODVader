package lodVader.spring.REST.models.links;

import java.util.ArrayList;
import java.util.Arrays;

import lodVader.API.diagram.Diagram;
import lodVader.API.diagram.DiagramData;
import lodVader.API.diagram.Link;
import lodVader.API.diagram.Node;
import lodVader.configuration.LODVaderProperties;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.mongodb.queries.LinksetQueries;

public class DiagramModel {

	String LINK_TYPE;

	boolean showOntologies = false;

	boolean showDistribution = true;

	double min = 0.001;

	double max = 1;

	public String graph;

	DiagramData diagramData = null;

	ArrayList<Integer> datasets = new ArrayList<Integer>();

	public Diagram diagramTemp;

	public DiagramModel(boolean showOntologies, double min, double max, String type, Integer datasets[]) {
		this.showOntologies = showOntologies;
		if (min < 0.2)
			this.min = 0.2;
		this.min = min;
		this.max = max;
		this.datasets = new ArrayList<Integer>(Arrays.asList(datasets));
		checkLinkTypes(type);
	}

	public void manageRequest() {

		diagramData = new DiagramData();
		new LinksetQueries().getLinksetsDegrees(diagramData, LINK_TYPE, min, max);

		ArrayList<DistributionDB> dis = new DistributionQueries().getSetOfDistributions(diagramData.distributionsID);
		for (DistributionDB distributionMongoDBObject : dis) {
			diagramData.loadedDistributions.put(distributionMongoDBObject.getLODVaderID(), distributionMongoDBObject);
		}
		diagramTemp = new Diagram();

		for (Integer id : datasets) {

			int currentLevel = 1;
			DatasetDB d = new DatasetDB(id);

			iterateDataset(d, diagramTemp, d.getLODVaderID(), currentLevel);
		}

		int[] results = new int[datasets.size()];

		for (int i = 0; i < results.length; i++) {
			try {
				results[i] = datasets.get(i);
			} catch (NumberFormatException nfe) {
			}
			;
		}

		diagramTemp.printSelectedBubbles(results);

	}

	private void iterateDataset(DatasetDB dataset, Diagram diagram, int parentDataset, int currentLevel) {

		for (DatasetDB subset : dataset.getSubsetsAsMongoDBObject()) {
			makeLink(diagram.addNode(new Node(dataset, true, parentDataset)),
					diagram.addNode(new Node(subset, true, parentDataset)), diagram, "S");
			iterateDataset(subset, diagram, parentDataset, --currentLevel);
		}

		for (DistributionDB distribution : dataset.getDistributionsAsMongoDBObjects()) {

			makeLink(diagram.addNode(new Node(dataset, true, parentDataset)),
					diagram.addNode(new Node(distribution, true, parentDataset)), diagram, "S");

			// get indegree and outdegree for a distribution
			ArrayList<LinksetDB> in = diagramData.indegreeLinks.get(distribution.getLODVaderID());
			ArrayList<LinksetDB> out = diagramData.outdegreeLinks.get(distribution.getLODVaderID());

			if (in != null) {

				for (LinksetDB linkset : in) {
					// get all distribution objects

					DistributionDB source = diagramData.loadedDistributions.get(linkset.getDistributionSource());

					DistributionDB target = distribution;

					String links = getLinksCorrectFormat(linkset);

					if (!showOntologies) {
						if (source.getIsVocabulary() == false && target.getIsVocabulary() == false)
							makeLink(diagram.addNode(new Node(source, showDistribution, parentDataset)),
									diagram.addNode(new Node(target, showDistribution, parentDataset)), diagram, links);
					} else
						makeLink(diagram.addNode(new Node(source, showDistribution, parentDataset)),
								diagram.addNode(new Node(target, showDistribution, parentDataset)), diagram, links);

				}
			}
			if (out != null)
				for (LinksetDB linkset : out) {

					DistributionDB source = distribution;
					DistributionDB target = diagramData.loadedDistributions.get(linkset.getDistributionTarget());

					String links = getLinksCorrectFormat(linkset);

					if (!showOntologies) {
						if (source.getIsVocabulary() == false && target.getIsVocabulary() == false)
							makeLink(diagram.addNode(new Node(source, showDistribution, parentDataset)),
									diagram.addNode(new Node(target, showDistribution, parentDataset)), diagram, links);
					} else
						makeLink(diagram.addNode(new Node(source, showDistribution, parentDataset)),
								diagram.addNode(new Node(target, showDistribution, parentDataset)), diagram, links);

				}
		}
	}

	private void makeLink(Node source, Node target, Diagram diagram, String link) {

		Link l = new Link(source, target, link);

		diagram.addNode(target);
		diagram.addNode(source);
		diagram.addLink(l);

	}

	protected String getLinksCorrectFormat(LinksetDB linkset) {
		String links;
		if (LINK_TYPE.equals(LinksetDB.INVALID_LINKS))
			links = linkset.getInvalidLinksAsString();
		else if (LINK_TYPE.equals(LinksetDB.PREDICATE_SIMILARITY))
			links = linkset.getPredicatesSimilarityAsString();
		else if (LINK_TYPE.equals(LinksetDB.LINK_STRENGHT)) {
			links = linkset.getStrengthAsString();

		} else
			links = linkset.getLinksAsString();

		return links;
	}

	protected void checkLinkTypes(String type) {
		if (type.equals("showLinksStrength"))
			LINK_TYPE = LinksetDB.LINK_STRENGHT;
		else if (type.equals("showDarkLOD")) {
			LINK_TYPE = LinksetDB.INVALID_LINKS;
			min = LODVaderProperties.LINKSET_TRESHOLD;
			max = -1;
		} else if (type.equals("showSimilarity"))
			LINK_TYPE = LinksetDB.PREDICATE_SIMILARITY;
		else if (type.equals("showLinks")) {
			LINK_TYPE = LinksetDB.LINK_NUMBER_LINKS;
			min = LODVaderProperties.LINKSET_TRESHOLD;
			max = -1;
		}

	}
}
