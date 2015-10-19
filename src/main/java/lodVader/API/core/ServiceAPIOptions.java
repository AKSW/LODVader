package lodVader.API.core;

import java.util.ArrayList;

public class ServiceAPIOptions extends ArrayList<APIOption>{
	
	private static final long serialVersionUID = -7797640635790893518L;

	public static final String ADD_DATASET = "addDataset";
	
	public static final String DATASET_STATUS = "datasetStatus";
	
	public static final String RETRIEVE_DATASET = "retrieveDataset"; 
	
	public static final String RDF_FORMAT = "rdfFormat"; 
	
	public static final String SERVER_STATISTICS = "statistics"; 

	public static final String COMPARE_DATASETS = "compareDatasets"; 

	public static final String COMPARE_DATASETS_DATASET1 = "compareDataset1"; 

	public static final String COMPARE_DATASETS_DATASET2 = "compareDataset2"; 
	
	public static final String DATASET_STATISTICS = "datasetStatistics"; 
	
	public static final String DATASET_DETAILS_STATISTICS = "datasetDetailsStatistics"; 

	public static final String DUMP_FILE = "dumpFile"; 
	
	public static final String TOP_N = "topN"; 
	
	public static final String TYPE = "type"; 
	
	public static final String DATASET_TYPE_LINKS = "links"; 
	
	public static final String DATASET_TYPE_STRENGTH = "strength"; 
	
	public static final String DATASET_TYPE_PREDICATES = "predicates"; 
	
	public static final String DATASET_TYPE_TYPE = "type"; 
	
	public static final String DATASET_TYPE_CLASSES = "class"; 
	
	public static final String DATASET_TYPE_SUBCLASSES = "subclass"; 
	
	public static final String LIST_LINKS = "links"; 
	
	public static final String LIST = "list"; 
	
	public static final String LIST_START = "start"; 
	
	public static final String LIST_SKIP = "length"; 
	
	public static final String LIST_SEARCH = "search[value]"; 
	
	public static final String LIST_SEARCH_VOCABULARIES = "searchVocabularies"; 

	public static final String LIST_SEARCH_SUBJECT = "searchSubject"; 

	public static final String LIST_SEARCH_PROPERTY = "searchProperty"; 

	public static final String LIST_SEARCH_OBJECT = "searchObject"; 

	
	
	{
		add(new APIOption(ADD_DATASET, "link for your dataset description to be streamed. Might be a list of links."));
		add(new APIOption(DATASET_STATUS, "The API parameter used to verify the details of the loading/streaming process for a dataset."));
		add(new APIOption(RETRIEVE_DATASET, "Retrieves RDF data about counted links in the VoID Linkset format."));
		add(new APIOption(RDF_FORMAT, "format of the added links in the addDataset parameter. Formats are: ttl, nt or rdfxml."));
		add(new APIOption(SERVER_STATISTICS, "Retrieve server statistics."));
		add(new APIOption(LIST, "Retruns a list dump files in the server."));
		add(new APIOption(LIST_START, ""));
		add(new APIOption(LIST_SKIP, ""));
		add(new APIOption(DATASET_STATISTICS, ""));
		add(new APIOption(COMPARE_DATASETS, ""));
		add(new APIOption(DATASET_DETAILS_STATISTICS, ""));
		
		
	}
	
}
