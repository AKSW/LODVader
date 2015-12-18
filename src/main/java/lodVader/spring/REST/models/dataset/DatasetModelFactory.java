package lodVader.spring.REST.models.dataset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasetModelFactory {
	
	final static Logger logger = LoggerFactory.getLogger(DatasetModelFactory.class);

	public static AddDatasetModel createDataset(String datasetURI, String format) {
				
		if (!DatasetThreadTasks.tasks.containsKey(datasetURI)) {
			AddDatasetModel instace = new AddDatasetModel(datasetURI, format);
			Thread thread = new Thread(instace);
			DatasetThreadTasks.tasks.put(datasetURI, instace);
			instace.setCoreMsg("API successfully initialized.");
			thread.start();		
			return instace;
			 
		} else
			return (AddDatasetModel) DatasetThreadTasks.tasks.get(datasetURI);
	}
}
