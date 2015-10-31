package lodVader.API.services;

import org.apache.log4j.Logger;

import lodVader.API.core.APITasks;
import lodVader.exceptions.LODVaderNoDatasetFoundException;
import lodVader.exceptions.api.DynamicLODAPINoLinksFoundException;

public class APIFactory {
	
	final static Logger logger = Logger.getLogger(APIFactory.class);

	public static APIDataset createDataset(String datasetURI, String format) {
		
//		logger.debug("API Factory Create Dataset started");
		
		if (!APITasks.tasks.containsKey(datasetURI)) {
			APIDataset instace = new APIDataset(datasetURI, format);
			APITasks.tasks.put(datasetURI, instace);
			instace.start();
			instace.apiMessage.setCoreMsgSuccess();			
			return instace;
			
		} else
			return (APIDataset) APITasks.tasks.get(datasetURI);
	}
	
	public static APIDatasetStatus createStatusDataset(String datasetURI) {
		
			return new APIDatasetStatus(datasetURI);
		
	}

	public static APIRetrieveRDF retrieveDataset(String datasetURI) throws LODVaderNoDatasetFoundException, DynamicLODAPINoLinksFoundException {
		return new APIRetrieveRDF(datasetURI);
	}
	
	public static APIRetrieveRDF retrieveDataset(String source, String target) throws LODVaderNoDatasetFoundException, DynamicLODAPINoLinksFoundException {
		return new APIRetrieveRDF(source, target);
	}

}
