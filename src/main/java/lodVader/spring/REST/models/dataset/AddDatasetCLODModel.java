package lodVader.spring.REST.models.dataset;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.jena.riot.RiotException;

import lodVader.Manager;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.exceptions.LODVaderNoDatasetFoundException;
import lodVader.parsers.descriptionfileparser.CLODFileParser;
import lodVader.parsers.descriptionfileparser.DataIDVoIDFileParser;
import lodVader.spring.REST.models.RESTMsg;

public class AddDatasetCLODModel extends RESTMsg implements Runnable {

	private String datasetURI = "http://cirola2000.cloudapp.net/files/urls";

	private String format = "nt";

	private String apiCall;

	private Boolean error = false;

	
	public AddDatasetCLODModel() {
		// TODO Auto-generated constructor stub
	}
	
	public void addDatasetsCLOD() {

		CLODFileParser inputRDFParser = new CLODFileParser();
		try {
			setCoreMsgSuccess();

			// read and parse description file
			inputRDFParser.readModel(datasetURI, format);

			if (inputRDFParser.distributionsLinks.size() > 0) {
				setParserMsg(
						inputRDFParser.distributionsLinks.size() + " distributions found. We are processing them!");

				// stream distributions
				Manager m = new Manager(inputRDFParser.distributionsLinks);
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
		}  catch (FileNotFoundException e) {
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
		addDatasetsCLOD();
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
