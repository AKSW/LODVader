package lodVader.API.services;

import java.io.IOException;

import org.apache.jena.riot.RiotException;

import lodVader.Manager;
import lodVader.API.core.API;
import lodVader.API.core.APITasks;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.exceptions.LODVaderNoDatasetFoundException;
import lodVader.parsers.descriptionfileparser.DescriptionFileParser;

public class APIDataset extends API {

	String datasetURI;
	String format;

	public APIDataset(String datasetURI, String format) {
		this.datasetURI = datasetURI;
		this.format = format;
	}

	public void addDatasets() {

		DescriptionFileParser inputRDFParser = new DescriptionFileParser();
		try {
			apiMessage.setCoreMsgSuccess();

			// read and parse description file
			inputRDFParser.readModel(datasetURI, format);
			inputRDFParser.parseDistributions();

			if (inputRDFParser.distributionsLinks.size() > 0) {
				apiMessage.setParserMsg(inputRDFParser.distributionsLinks
						.size()
						+ " distributions found. We are processing them!");
				
				// stream distributions
				Manager m = new Manager(inputRDFParser.distributionsLinks);
			} else {
				apiMessage.setParserMsg("No datasets found.");

			}
		} catch (LODVaderNoDatasetFoundException e) {
			apiMessage.setParserMsg(e.getMessage(), true);
			e.printStackTrace();

		} catch (RiotException e) {
			apiMessage.setParserMsg("Bad format file. ", true);
			e.printStackTrace();
		} catch (LODVaderFormatNotAcceptedException e) {
//			 apiMessage.setParserMsg(e.getMessage(), true);
			 apiMessage.setParserMsg("", true);
			e.printStackTrace();
		} catch (LODVaderLODGeneralException e) {
			apiMessage.setParserMsg(e.getMessage(), true);
			e.printStackTrace();
		} catch (IOException e) {
			apiMessage.setParserMsg(e.getMessage(), true);
			e.printStackTrace();
		}

		APITasks.tasks.remove(datasetURI);

	}

	public void run() {
		addDatasets();
	}
}
