package lodVader.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * General properties of the application. Most of them are 
 * set in the config file.
 * @author ciro
 *
 */

@Component
public class LODVaderProperties {
	
	@Autowired
	Config conf;

	/**
	 * Load properties from file
	 */
	public void loadProperties() {
		
			// get the property value and print it out
		    BASE_PATH = conf.getProperties().getPathProperties().getBasePath();
			NR_THREADS = conf.getProperties().getPathProperties().getNrThreads();
			LOV_URL = conf.getProperties().getPathProperties().getLovUrl();
			RESUME = conf.getProperties().getPathProperties().getResume();
			RESUME_ERRORS = conf.getProperties().getPathProperties().getResumeErrors();
			REMOVE_DATASET_PASS = conf.getProperties().getPathProperties().getRemoveDatasetPass();
			
			try{
				FPP_EQUATION = null;
			}
			catch(Exception e){
				e.printStackTrace();
			}
			
			USE_MULTITHREAD = conf.getProperties().getPathProperties().getMultithread();
			FILTER_PATH = BASE_PATH + "filters/";
			SUBJECT_PATH = BASE_PATH + "subjects/";
			OBJECT_PATH = BASE_PATH + "objects/";
			TMP_FOLDER = BASE_PATH + "tmp/";
			DUMP_PATH = BASE_PATH + "dump/";
			FILE_URL_PATH = BASE_PATH + "dataid/";
			AUTHORITY_FILTER_PATH = BASE_PATH + "authority_filter";
			DISTRIBUTION_PREFIX = "distribution_";
			SUBJECT_FILE_DISTRIBUTION_PATH = SUBJECT_PATH
					+ "subject_distribution_";
			OBJECT_FILE_DISTRIBUTION_PATH = OBJECT_PATH
					+ "object_distribution_";
			SUBJECT_FILE_FILTER_PATH = FILTER_PATH + "subject_filter_";
			OBJECT_FILE_FILTER_PATH = FILTER_PATH + "object_filter_";
			OBJECT_FILE_LOV_PATH = FILTER_PATH + "lov_object";
			SUBJECT_FILE_LOV_PATH = FILTER_PATH + "lov_subject";
			FILTER_FILE_LOV_PATH = FILTER_PATH + "lov_filter";
			

	}

	// defining what path should be used to store files
	public static String BASE_PATH;

	// defining filter path
	public static String FILTER_PATH;
	public static String AUTHORITY_FILTER_PATH;
	
	// defining subject file path
	public static String SUBJECT_PATH;

	// defining object file path
	public static String OBJECT_PATH;
	
	// defining dump file path
	public static String DUMP_PATH;
	
	
	// defining folder for temporary files
	public static String TMP_FOLDER;
	
	
	// defining max number of threads
	public static Integer NR_THREADS;

	// defining dataids file path
	public static String FILE_URL_PATH;

	// defining dataset file suffix
	public static String DISTRIBUTION_PREFIX;
	
	public static boolean RESUME;

	public static boolean RESUME_ERRORS;

	// defining file names for distributions after separate subject and object
	public static String SUBJECT_FILE_DISTRIBUTION_PATH;
	public static String OBJECT_FILE_DISTRIBUTION_PATH;

	// defining file names for filters for subjects and objects
	public static String SUBJECT_FILE_FILTER_PATH;
	public static String OBJECT_FILE_FILTER_PATH;
	
	// defining path for LOV
	public static String SUBJECT_FILE_LOV_PATH;
	public static String OBJECT_FILE_LOV_PATH;
	public static String FILTER_FILE_LOV_PATH;

	// defining server properties
	public static final String MESSAGE_INFO = "info";
	public static final String MESSAGE_LOG = "log";
	public static final String MESSAGE_WARN = "warn";
	public static final String MESSAGE_ERROR = "error";

	// mongodb properties
//	public static String MONGODB_HOST;
//	public static int MONGODB_PORT;
//	public static String MONGODB_DB;
//	public static Boolean MONGODB_SECURE_MODE;
//	public static String MONGODB_USERNAME;
//	public static String MONGODB_PASSWORD;

	// fpp equation
	public static String FPP_EQUATION = null;
	
	
	// other properties
	public static Boolean USE_MULTITHREAD;
	public static String LOV_URL;
	public static String REMOVE_DATASET_PASS;
	
	public static int MAX_CHUNK_SIZE = 500000;
	
	public static int TOP_N_LINKS = 100;

	public static int BF_BUFFER_RANGE = 500000;

	public static int LINKSET_TRESHOLD = 1;
	
//	public static int CHECK_LINKS_EACH = 15000;
	public static int CHECK_LINKS_EACH = 100000;
	
	public static boolean CHECK_LOV = true;

	public static boolean ONLY_STREAM_DATASETS_AND_SAVE_NT_FORMAT = false;

	public static String VERSION = "v1.3";

	public static int MONGODB_MINIMUM_MAJOR_VERSION = 2;
	public static int MONGODB_MINIMUM_MINOR_VERSION = 6;
	
	public static boolean EVALUATE_LINKS = false;

	public static String EVALUATE_LINKS_PATH = "";
	public static String EVALUATE_COHESION_PATH = "/home/ciro/dataid/LODVader-Spring-CLOD-Data/";


}
