package lodVader.API.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import lodVader.API.core.APIOption;
import lodVader.API.core.ServiceAPIOptions;
import lodVader.API.services.APIDataset;
import lodVader.API.services.APIFactory;
import lodVader.API.services.APIRetrieveRDF;
import lodVader.API.services.APIStatistics;
import lodVader.API.services.APIStatus;
import lodVader.exceptions.LODVaderNoDatasetFoundException;
import lodVader.exceptions.api.DynamicLODAPINoLinksFoundException;
import lodVader.exceptions.api.DynamicLODAPINoParametersFoundExceiption;

public class ServiceAPI extends HttpServlet {
	
	final static Logger logger = Logger.getLogger(ServiceAPI.class);

	static HttpServletRequest staticRequest;

	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	public static String getServerURL() {
		return staticRequest.getRequestURL().toString();
	}

	private void manageRequest(HttpServletRequest request,
			HttpServletResponse response) throws IOException {
		staticRequest = request;

		ServiceAPIOptions options = new ServiceAPIOptions();
//		PrintWriter out;
		ServletOutputStream out = response.getOutputStream();

//		out = response.getWriter();
		try {

			Map<String, String[]> parameters = request.getParameterMap();

			// check whether there is at least one valid parameter
			boolean hasOption = false;
			Iterator<APIOption> it = options.iterator();
			while (it.hasNext()) {
				if (parameters.containsKey(it.next().getOption()))
					hasOption = true;
			}

			if (!hasOption)
				throw new DynamicLODAPINoParametersFoundExceiption();

			if (parameters.containsKey(ServiceAPIOptions.ADD_DATASET)) {
				String format;
				if (parameters.containsKey(ServiceAPIOptions.RDF_FORMAT)) {
					format = (parameters.get(ServiceAPIOptions.RDF_FORMAT)[0].toString());
				} else{
					format = "rdfxml";
				}

				for (String datasetURI : parameters.get(ServiceAPIOptions.ADD_DATASET)) {
					
//					logger.debug("API ADD_DATASET: "+datasetURI+ format);

					APIDataset apiDataset = APIFactory.createDataset(
							datasetURI, format);
					
					while (!apiDataset.apiMessage.hasParserMsg()){
						try {
							Thread.sleep(10);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					out.write(apiDataset.apiMessage.toJSONString().getBytes("UTF-8"));
					out.write("\n".getBytes("UTF-8"));
				}
			}

			if (parameters.containsKey(ServiceAPIOptions.DATASET_STATUS)) {

				for (String datasetURI : parameters.get(ServiceAPIOptions.DATASET_STATUS)) {
					
//					logger.debug("API DATASET_STATUS: "+datasetURI);

					APIStatus apiStatus = APIFactory
							.createStatusDataset(datasetURI);
					try {
						if (apiStatus != null) {
							out.write(apiStatus.apiMessage.toJSONString().getBytes("UTF-8"));
							out.write("\n".getBytes("UTF-8"));
						} else {
							out.write("Error: we couldn't find your dataset. ".getBytes("UTF-8"));
							out.write("\n".getBytes("UTF-8"));
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			if (parameters.containsKey(ServiceAPIOptions.RETRIEVE_DATASET)) {
				for (String datasetURI : parameters
						.get(ServiceAPIOptions.RETRIEVE_DATASET)) {
					APIRetrieveRDF apiRetrieve = null;
					
					if(parameters
						.containsKey("source")  && parameters
						.containsKey("target")){
						apiRetrieve = APIFactory
								.retrieveDataset(parameters
										.get("source")[0],
										parameters
										.get("target")[0]);
					}
					else
					apiRetrieve = APIFactory
							.retrieveDataset(datasetURI);
					
					apiRetrieve.outModel.write(out, "TURTLE");
				}
			}
			if (parameters.containsKey(ServiceAPIOptions.SERVER_STATISTICS)) {
				out.write(new APIStatistics().getStatistics().toJSONString().getBytes("UTF-8")); 
			}			
			if (parameters.containsKey(ServiceAPIOptions.LIST)) {
				int searchVocabularies = Integer.parseInt(parameters.get(ServiceAPIOptions.LIST_SEARCH_VOCABULARIES)[0].toString());
				String searchValue = parameters.get(ServiceAPIOptions.LIST_SEARCH)[0];
				
				String searchSubject = null;
				String searchProperty = null;
				String searchObject = null;
				
				if(!parameters.get(ServiceAPIOptions.LIST_SEARCH_SUBJECT)[0].equals(""))
					searchSubject = parameters.get(ServiceAPIOptions.LIST_SEARCH_SUBJECT)[0].toString();
				if(!parameters.get(ServiceAPIOptions.LIST_SEARCH_PROPERTY)[0].equals(""))
					searchProperty = parameters.get(ServiceAPIOptions.LIST_SEARCH_PROPERTY)[0].toString();
				if(!parameters.get(ServiceAPIOptions.LIST_SEARCH_OBJECT)[0].equals(""))
					searchObject = parameters.get(ServiceAPIOptions.LIST_SEARCH_OBJECT)[0].toString();
					
				out.write(new APIStatistics().listDistributions(
						Integer.parseInt(parameters.get(ServiceAPIOptions.LIST_START)[0]),
						Integer.parseInt(parameters.get(ServiceAPIOptions.LIST_SKIP)[0]), searchVocabularies,
						searchValue, searchSubject, searchProperty, searchObject
						).toJSONString().getBytes("UTF-8")); 
			}
			
			if (parameters.containsKey(ServiceAPIOptions.DATASET_STATISTICS)) {
				out.write(new APIStatistics().datasetDetails(parameters.get(ServiceAPIOptions.DUMP_FILE)[0],
						Integer.parseInt(parameters.get(ServiceAPIOptions.TOP_N)[0]),
						parameters.get(ServiceAPIOptions.TYPE)[0]
						).toJSONString().getBytes("UTF-8")); 
			}
			if (parameters.containsKey(ServiceAPIOptions.COMPARE_DATASETS)) {
				out.write(new APIStatistics().compareDatasets(Integer.valueOf(parameters.get(ServiceAPIOptions.COMPARE_DATASETS_DATASET1)[0]),
						Integer.valueOf(parameters.get(ServiceAPIOptions.COMPARE_DATASETS_DATASET2)[0]), parameters.get(ServiceAPIOptions.TYPE)[0]).toJSONString().getBytes("UTF-8")); 
			}	
			
			if (parameters.containsKey(ServiceAPIOptions.DATASET_DETAILS_STATISTICS)) {
				out.write(new APIStatistics().getTop(parameters.get(ServiceAPIOptions.DUMP_FILE)[0],
						Integer.parseInt(parameters.get(ServiceAPIOptions.TOP_N)[0]),
						parameters.get(ServiceAPIOptions.TYPE)[0]
						).toJSONString().getBytes("UTF-8")); 			
			}	
			
			

		} catch (DynamicLODAPINoParametersFoundExceiption e) {
			Iterator<APIOption> it = options.iterator();
			out.write("We couldn't find any valid parameter.\n\n\n".getBytes("UTF-8"));

			out.write("Parameter \t\t Description\n\n".getBytes("UTF-8"));

			while (it.hasNext()) {
				APIOption o = it.next();
				out.write((o.getOption() + "\t\t" + o.getDescription() + "\n").getBytes("UTF-8"));
			}

			out.write("\n\n\nFor full documentation please access: http://dynamiclod.dbpedia.org/wiki.html".getBytes("UTF-8"));
		} catch (LODVaderNoDatasetFoundException e) {
			out.write(e.getMessage().getBytes("UTF-8"));
		} catch (DynamicLODAPINoLinksFoundException e) {
			out.write(e.getMessage().getBytes("UTF-8"));

		}
	}
}
