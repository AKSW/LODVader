package lodVader.spring.REST.models.dataset;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.apache.jena.riot.RiotException;

import lodVader.Manager;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.exceptions.LODVaderNoDatasetFoundException;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.parsers.descriptionfileparser.CLODFileParser;
import lodVader.parsers.descriptionfileparser.DataIDVoIDFileParser;
import lodVader.spring.REST.models.RESTMsg;

public class AddDatasetModel extends RESTMsg implements Runnable {

	private String datasetURI;

	private String format;

	private String apiCall;

	private Boolean error = false;

	public AddDatasetModel(String datasetURI, String format) {
		this.datasetURI = datasetURI;
		this.format = format;
	}

	public void addDatasets() {

		DataIDVoIDFileParser inputRDFParser = new DataIDVoIDFileParser();
		try {
			setCoreMsgSuccess();

			// read and parse description file
			inputRDFParser.readModel(datasetURI, format);
			inputRDFParser.parseDistributions();

			if (inputRDFParser.distributionsLinks.size() > 0) {
				setParserMsg(
						inputRDFParser.distributionsLinks.size() + " distributions found. We are processing them!");

				// get datasets to instantiate a Manager
				HashMap<Integer, DatasetDB> datasets = new HashMap<Integer, DatasetDB>();
				for(DistributionDB dist: inputRDFParser.distributionsLinks)
					datasets.put(dist.getTopDatasetID(), new DatasetDB(dist.getTopDatasetID()));
				
				// stream distributions
				Manager m = new Manager(datasets.values());
			} else {
				setParserMsg("No datasets found.");

			}
		} catch (LODVaderNoDatasetFoundException e) {
			setParserMsg(e.getMessage());
			error = true;
			e.printStackTrace();

		} catch (RiotException e) {
			setParserMsg("Bad file format.");
			error = true;
			e.printStackTrace();
		} catch (LODVaderFormatNotAcceptedException e) {
			setParserMsg(e.getMessage());
			error = true;
			e.printStackTrace();
		} catch (LODVaderLODGeneralException e) {
			setParserMsg(e.getMessage());
			error = true;
			e.printStackTrace();

		} catch (FileNotFoundException e) {
			setParserMsg("Impossible to reach the file: " + e.getMessage());
			error = true;
			e.printStackTrace();

		} catch (UnknownHostException e) {
			setParserMsg("Unknown Host: " + e.getMessage());
			error = true;
			e.printStackTrace();
		} catch (IOException e) {
			setParserMsg(e.getMessage());
			error = true;
			e.printStackTrace();
		}

	}
	
	public void run() {
		addDatasets();
	}

	public String getDatasetURI() {
		return datasetURI;
	}

	public void setDatasetURI(String datasetURI) {
		this.datasetURI = datasetURI;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public Boolean getError() {
		return error;
	}

	public void setError(Boolean error) {
		this.error = error;
	}

	public String getApiCall() {
		return apiCall;
	}

	public void setApiCall(String apiCall) {
		this.apiCall = apiCall;
	}

}
