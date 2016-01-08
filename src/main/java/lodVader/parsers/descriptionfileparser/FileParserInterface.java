package lodVader.parsers.descriptionfileparser;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.jena.riot.RiotException;

import lodVader.exceptions.LODVaderNoDatasetFoundException;

public interface FileParserInterface {
	
	public String readModel(String URL, String format)
			throws MalformedURLException, IOException,
			LODVaderNoDatasetFoundException, RiotException;
}
