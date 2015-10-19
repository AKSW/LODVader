package lodVader.API.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import lodVader.LODVaderProperties;
import lodVader.API.diagram.Bubble;
import lodVader.API.diagram.Diagram;
import lodVader.API.diagram.DiagramData;
import lodVader.API.diagram.Link;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.mongodb.queries.NSQueries;
import lodVader.mongodb.queries.LinksetQueries;

public class CreateD3JSONFormat extends HttpServlet {

	private static final long serialVersionUID = -7213269624452749676L;

	String LINK_TYPE;

	boolean showDistribution = true;
	
	boolean showOntologies = false;
	
	boolean showInvalidLinks = false;
	
	double min = 0.001;
	
	double max = 1;
	
	DiagramData diagramData = null;
	
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}
	

	public void printOutput(JSONArray nodes, JSONArray links,
			HttpServletResponse response) {

		JSONObject obj = new JSONObject();

		obj.put("nodes", nodes);
		obj.put("links", links);

		try {
			ServletOutputStream out = response.getOutputStream();
			out.write(obj.toString().getBytes("UTF-8"));
//			response.getWriter().print(obj);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void manageRequest(HttpServletRequest request,
			HttpServletResponse response) {

		JSONArray nodes = new JSONArray();
		JSONArray links = new JSONArray();

		Map<String, String[]> parameters = request.getParameterMap();
		

		showDistribution = checkParamenter(parameters, "showDistributions");
		showOntologies = checkParamenter(parameters, "showOntologies");
		checkRange(parameters);
		checkLinkTypes(parameters);
		
		diagramData = new DiagramData();
		new LinksetQueries()
		.getLinksetsDegrees(diagramData, LINK_TYPE, min, max);
		
		// get indegree and outdegree for a distribution
//		indegreeLinks = new LinksetQueries()
//				.getLinksetsInDegreeByDistribution(LINK_TYPE, min, max);
//		outdegreeLinks = new LinksetQueries()
//				.getLinksetsOutDegreeByDistribution(LINK_TYPE, min, max);
		
		
	
		// load distributions buffer
//		for (int distribution : indegreeLinks.keySet()) {
//				distributionsID.add(distribution);
//		}
//		
//		for (int distribution : outdegreeLinks.keySet()) {
//			if(!loadedDistributions.containsKey(distribution)){
//				distributionsID.add(distribution);
//			}
//		}
		ArrayList<DistributionDB> dis = new DistributionQueries().getSetOfDistributions(diagramData.distributionsID);
		for (DistributionDB distributionMongoDBObject : dis) {
			diagramData.loadedDistributions.put(distributionMongoDBObject.getLODVaderID(), distributionMongoDBObject);
		}
			
		
		if (parameters.containsKey("getAllDistributions")) {

			Diagram diagram = new Diagram();
			ArrayList<LinksetDB> linksets = new LinksetQueries()
					.getLinksetsWithLinks();

			for (LinksetDB linkset : linksets) {

				Bubble target = new Bubble( new DistributionDB(
						linkset.getDistributionTarget()));
				Bubble source = new Bubble( new DistributionDB(
						linkset.getDistributionSource()));
			

				Link link = new Link(source, target, linkset.getLinksAsString());

				diagram.addBubble(target);
				diagram.addBubble(source);

				diagram.addLink(link);
			}

			nodes = diagram.getBubblesJSON();
			links = diagram.getLinksJSON();
			printOutput(nodes, links, response);
		}

		if (parameters.containsKey("dataset")) {
			Diagram diagramTemp = new Diagram();
			
			
			for (String datasetID : parameters.get("dataset")) {

				int currentLevel = 1;
//				if (parameters.containsKey("level")) {
//					currentLevel = Integer.parseInt(parameters.get("level")[0]);
//				}
				
				DatasetDB d = new DatasetDB(Integer.valueOf(datasetID));

				iterateDataset(d, diagramTemp, d.getLODVaderID(), currentLevel);				
			}

			int[] results = new int[parameters.get("dataset").length];

			for (int i = 0; i < results.length; i++) {
			    try {
			        results[i] = Integer.parseInt(parameters.get("dataset")[i]);
			    } catch (NumberFormatException nfe) {};
			}
			
			diagramTemp.printSelectedBubbles(results);
			
			nodes = diagramTemp.getBubblesJSON();
			links = diagramTemp.getLinksJSON();

			printOutput(nodes, links, response);
		}

	}

	private void iterateDataset(DatasetDB dataset,
			Diagram diagram, int parentDataset, int currentLevel) {
		
		
		for (DatasetDB subset : dataset.getSubsetsAsMongoDBObject()) {
			makeLink(diagram.addBubble(new Bubble(dataset, true,parentDataset)),
					diagram.addBubble(new Bubble(subset, true,parentDataset)), diagram, "S");				
			iterateDataset(subset, diagram, parentDataset, --currentLevel);
		}
		
		for (DistributionDB distribution : dataset.getDistributionsAsMongoDBObjects()) {
			

			makeLink(diagram.addBubble(new Bubble(dataset, true,parentDataset)),
					diagram.addBubble(new Bubble(distribution, true,parentDataset)), diagram, "S");

//			// get indegree and outdegree for a distribution
//			ArrayList<LinksetMongoDBObject> in = new LinksetQueries()
//					.getLinksetsInDegreeByDistribution(distribution.getDynLodID(), LINK_TYPE,min, max);
//			ArrayList<LinksetMongoDBObject> out = new LinksetQueries()
//					.getLinksetsOutDegreeByDistribution(distribution.getDynLodID(), LINK_TYPE,min, max);
			
			// get indegree and outdegree for a distribution
			ArrayList<LinksetDB> in = diagramData.indegreeLinks.get(distribution.getLODVaderID());
			ArrayList<LinksetDB> out = diagramData.outdegreeLinks.get(distribution.getLODVaderID());
			

			if(in!=null){
				
			for (LinksetDB linkset : in) {
				// get all distribution objects
				
				
//				DistributionMongoDBObject source =  new DistributionMongoDBObject(linkset.getDistributionSource());
				DistributionDB source = diagramData.loadedDistributions.get(linkset.getDistributionSource());
				
				
//				DistributionMongoDBObject target =  new DistributionMongoDBObject(linkset.getDistributionTarget());
				DistributionDB target =  distribution;
				
				String links = getLinksCorrectFormat(linkset);
				
				if(!showOntologies){
				if(source.getIsVocabulary() == false && target.getIsVocabulary() == false)
					makeLink(diagram.addBubble(new Bubble(source, showDistribution,parentDataset)),
							diagram.addBubble(new Bubble(target, showDistribution,parentDataset)), diagram, links);
				}
				else
					makeLink(diagram.addBubble(new Bubble(source, showDistribution,parentDataset)),
							diagram.addBubble(new Bubble(target, showDistribution,parentDataset)), diagram, links);

			}
		}
			if(out!=null)
			for (LinksetDB linkset : out) {
//				DistributionMongoDBObject source =  new DistributionMongoDBObject(linkset.getDistributionSource());
				DistributionDB source =  distribution;
//				DistributionMongoDBObject target =  new DistributionMongoDBObject(linkset.getDistributionTarget());
				DistributionDB target =  diagramData.loadedDistributions.get(linkset.getDistributionTarget());
				
				
				String links = getLinksCorrectFormat(linkset);
				
				if(!showOntologies){
					if(source.getIsVocabulary() == false && target.getIsVocabulary() == false  )				
						makeLink(diagram.addBubble(new Bubble(source, showDistribution,parentDataset)),
								diagram.addBubble(new Bubble(target, showDistribution,parentDataset)), diagram, links);
				}
				else
						makeLink(diagram.addBubble(new Bubble(source, showDistribution,parentDataset)),
								diagram.addBubble(new Bubble(target, showDistribution,parentDataset)), diagram, links);
				
			}
		}
	}
	

	private void makeLink(Bubble source, Bubble target, Diagram diagram, String link){
		
		
		Link l = new Link(source, target, link);

		diagram.addBubble(target);
		diagram.addBubble(source);
		
		diagram.addLink(l);
		
//		if(LINK_TYPE.equals(LinksetMongoDBObject.LINK_STRENGHT) || LINK_TYPE.equals(LinksetMongoDBObject.LINK_SIMILARITY)){
//			if(!link.equals("S")){
//			
//					Link l = new Link(source, target, link);
//					diagram.addBubble(target);
//					diagram.addBubble(source);
//					
//					diagram.addLink(l);
//			}
//			else{
//				Link l = new Link(source, target, link);
//
//				diagram.addBubble(target);
//				diagram.addBubble(source);
//				
//				diagram.addLink(l);
//			}
//		}
//		else if(LINK_TYPE.equals(LinksetMongoDBObject.LINK_NUMBER_LINKS)){
//			Link l = new Link(source, target, link);
//
//			diagram.addBubble(target);
//			diagram.addBubble(source);
//			
//			diagram.addLink(l);
//		}
		
	}
	
	protected String getLinksCorrectFormat(LinksetDB linkset){
		String links;
		if(LINK_TYPE.equals(LinksetDB.INVALID_LINKS))
			links = linkset.getInvalidLinksAsString();
		else if(LINK_TYPE.equals(LinksetDB.PREDICATE_SIMILARITY))
			links = linkset.getPredicatesSimilarityAsString();
		else if(LINK_TYPE.equals(LinksetDB.LINK_STRENGHT)){
			links = linkset.getStrengthAsString();
			
		}
		else
			links = linkset.getLinksAsString();
		
		return links;
	}
	
	protected boolean checkParamenter(Map<String, String[]> parameters, String parameter){
		if (parameters.containsKey(parameter)) 
			return true;
		else
			return false;
	}
	
	protected void checkRange(Map<String, String[]> parameters){
		if (parameters.containsKey("linkFrom")) {
			min = Double.parseDouble(parameters.get("linkFrom")[0]);
			if (min<0.2)
				min=0.2;
		}
		if (parameters.containsKey("linkTo")) 
			max = Double.parseDouble(parameters.get("linkTo")[0]);
	}
	
	protected void checkLinkTypes(Map<String, String[]> parameters){
		if(checkParamenter(parameters, "linkType")){
//			if(parameters.get("linkType")[0].equals("showInvalidLinks"))
//				showInvalidLinks = true;
			
			if(parameters.get("linkType")[0].equals("showLinksStrength"))
				LINK_TYPE = LinksetDB.LINK_STRENGHT;
			else if(parameters.get("linkType")[0].equals("showDarkLOD")){
				LINK_TYPE = LinksetDB.INVALID_LINKS;
				min = 50;
				max = -1;
			}
			else if(parameters.get("linkType")[0].equals("showSimilarity"))
				LINK_TYPE = LinksetDB.PREDICATE_SIMILARITY;
			else if(parameters.get("linkType")[0].equals("showLinks")){
				LINK_TYPE = LinksetDB.LINK_NUMBER_LINKS;
				min = 50;
				max = -1;
			}
				
		}
	}
	
	
	
}
