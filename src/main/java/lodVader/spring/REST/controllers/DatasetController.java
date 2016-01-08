package lodVader.spring.REST.controllers;

import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.queries.DatasetQueries;
import lodVader.spring.REST.models.dataset.AddDatasetModel;
import lodVader.spring.REST.models.dataset.DatasetModelFactory;
import lodVader.spring.REST.models.dataset.StatusModel;

@RestController
public class DatasetController {

	/**
	 * Get details of a specific dataset
	 * @param id
	 * @return
	 */
	@RequestMapping(value = "/dataset/{id}", method = RequestMethod.GET)
	public DatasetDB dataset(@PathVariable int id) {
		return new DatasetDB(id);
	}

	@RequestMapping(value = "/dataset/list")
	public ArrayList<DatasetDB> list() {
		return new DatasetQueries().getDatasets(false);
	}

	@RequestMapping(value = "/dataset/size")
	public int size() {
		return new DatasetQueries().getDatasets(false).size();
	}

	@RequestMapping(value = "/dataset/status", produces=MediaType.APPLICATION_JSON_VALUE)
	public StatusModel status(@RequestParam(value = "dataset", required = true) String datasetAddress) {
		return new StatusModel(datasetAddress);
	}

	
	
	@RequestMapping(value = "/dataset/addCLOD")
	public void add2(HttpServletRequest request) {

		DatasetModelFactory.createDatasetCLOD();
		
		
	}
	
	@RequestMapping(value = "/dataset/add")
	public AddDatasetModel add(@RequestParam(value = "descriptionFileURL", required = true) String descriptionFileURL,
			@RequestParam(value = "format", required = true) String format, HttpServletRequest request) {

		AddDatasetModel datasetModel = DatasetModelFactory.createDataset(descriptionFileURL, format);
		datasetModel.setApiCall(request.getRequestURL() + "/?" + request.getQueryString());

		// wait for a while and show the parser msg
		while (datasetModel.getParserMsg() == null) {
			try {
				Thread.sleep(10); 
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return datasetModel;
	}

}
