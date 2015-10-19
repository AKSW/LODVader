package lodVader.API.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.queries.DatasetQueries;

public class ResourceTree extends HttpServlet {
	
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		getTree(request, response);
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		getTree(request, response);
	}


	ArrayList<DatasetDB> d = new ArrayList<DatasetDB>();

	public void getTree(HttpServletRequest request,
			HttpServletResponse response) {
		
		Map<String, String[]> parameters = request.getParameterMap();
		if(parameters.containsKey("linkedDatasets"))
			d = new DatasetQueries().getDatasetsNotVocabWithLinks();
		else
			d = new DatasetQueries().getTopDatasetsNotVocab();
		

		JsonArray data2 = new JsonArray();

		JsonArray datasetArray = new JsonArray();
		for (DatasetDB dataset : d) {
			ArrayList<DatasetDB> parent_list = dataset.getSubsetsAsMongoDBObject();
			
			// add dataset
			JsonObject jsonparent = new JsonObject();
			jsonparent.put("parent", "#");					
			jsonparent.put("id", dataset.getLODVaderID());
			jsonparent.put("text", dataset.getTitle());
			datasetArray.add(jsonparent);
			
			for (DatasetDB parent : parent_list) {
				for(int p: parent.getParentDatasetID()){
					if(p>0){
						jsonparent = new JsonObject();
						jsonparent.put("parent", p);					
						jsonparent.put("id", parent.getLODVaderID());
						jsonparent.put("text", parent.getTitle());
						datasetArray.add(jsonparent);
						break;
					}
				}
				
			}
			
			
			
//			ArrayList<Integer> parent_list = dataset.getParentDatasetID();
//			for (Integer parent : parent_list) {
//				JsonObject jsonparent = new JsonObject();
//				jsonparent.put("parent", parent);
//				jsonparent.put("id", dataset.getDynLodID());
//				jsonparent.put("text", dataset.getTitle());
//				datasetArray.add(jsonparent);
//				
//			}
			
			
//			List<String> distribution_list = dataset.getDistributionsURIs();
//				for (String distribution : distribution_list) {
//					JsonObject jsondistribution = new JsonObject();
//					DistributionMongoDBObject d = new DistributionMongoDBObject(distribution);
//					jsondistribution.put("parent", dataset.getUri());
//					jsondistribution.put("id", d.getUri());
//					jsondistribution.put("text", d.getTitle() +" (Distribution)");
//					datasetArray.add(jsondistribution);
//			}
//			if (parent_list.size() == 0) {
//				jsonparent = new JsonObject();
//				jsonparent.put("parent", "#");
//
//				jsonparent.put("id", dataset.getDynLodID());
//				
//				jsonparent.put("text", dataset.getTitle());
//				datasetArray.add(jsonparent);
//			}
			data2.add(datasetArray);
			
		}
	
		try {
			
			ServletOutputStream out = response.getOutputStream();
			out.write(datasetArray.toString().getBytes("UTF-8"));
//			response.getWriter().print(datasetArray.toString().getBytes("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
