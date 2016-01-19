package lodVader.spring.REST.controllers;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lodVader.spring.REST.models.degree.IndegreeDatasetModel;
import lodVader.spring.REST.models.degree.IndegreeModel;
import lodVader.spring.REST.models.degree.OutdegreeDatasetModel;
import lodVader.spring.REST.models.degree.OutdegreeModel;

@RestController
public class DegreeController {

	/**
	 * Valid distribution Links
	 */

	@RequestMapping(value = "/linkset/valid/distributions/indegree", produces = MediaType.APPLICATION_JSON_VALUE)
	public String indegree(@RequestParam(value = "n", required = false, defaultValue = "0") int n) {

		IndegreeModel model = new IndegreeModel();

		model.mapIndegreeWithVocabs = "function() { "
				+ "if ( this.links > 0 && this.distributionSourceIsVocabulary == true )"
				+ "emit(this.distributionTarget, {'distribution': this.distributionTarget, " + "'totalIndegree': 1,"
				+ "'links': this.links});" + "};";

		model.mapIndegreeNoVocabs = "function() { "
				+ "if ( this.links > 0 && this.distributionSourceIsVocabulary == false )"
				+ "emit(this.distributionTarget, {'distribution': this.distributionTarget, " + "'totalIndegree': 1,"
				+ "'links': this.links});" + "};";

		model.reduceInDegree = "function(key, values) {" + "var linksSum = 0;" + "var distributionSum = 0;"
				+ "values.forEach(function(linkset) {" + "linksSum += linkset.links;" + "distributionSum += 1;" + "});"
				+ "return {'distribution': key, 'totalIndegree':distributionSum,'links':linksSum};" + "};";

		model.mapReduceInDegree(n);

		return model.result.toString();
	}

	@RequestMapping(value = "/linkset/valid/distributions/outdegree", produces = MediaType.APPLICATION_JSON_VALUE)
	public String outdegree(@RequestParam(value = "n", required = false, defaultValue = "0") int n) {
		OutdegreeModel model = new OutdegreeModel();

		model.mapOutdegreeWithVocabs = "function() { "
				+ "if ( this.links > 0 && this.distributionTargetIsVocabulary == true )"
				+ "emit(this.distributionSource, {'distribution': this.distributionSource, " + "'totalOutdegree': 1,"
				+ "'links': this.links});" + "};";

		model.mapOutdegreeNoVocabs = "function() { "
				+ "if ( this.links > 0 && this.distributionTargetIsVocabulary == false )"
				+ "emit(this.distributionSource, {'distribution': this.distributionSource, " + "'totalOutdegree': 1,"
				+ "'links': this.links});" + "};";

		model.reduceOutDegree = "function(key, values) {" + "var linksSum = 0;" + "var distributionSum = 0;"
				+ "values.forEach(function(linkset) {" + "linksSum += linkset.links;" + "distributionSum += 1;" + "});"
				+ "return {'distribution': key, 'totalOutdegree':distributionSum,'links':linksSum};" + "};";

		model.mapReduceOutDegree(n);

		return model.result.toString();

	}

	/**
	 * Valid dataset Links
	 */

	@RequestMapping(value = "/linkset/dead/datasets/indegree", produces = MediaType.APPLICATION_JSON_VALUE)
	public String indegreeDataset(@RequestParam(value = "n", required = false, defaultValue = "0") int n) {

		IndegreeDatasetModel model = new IndegreeDatasetModel();
		model.isDeadLinks = true;
		model.calc();
		return model.result.toString();

	}

	@RequestMapping(value = "/linkset/valid/datasets/outdegree", produces = MediaType.APPLICATION_JSON_VALUE)
	public String outdegreeDatasets(@RequestParam(value = "n", required = false, defaultValue = "0") int n) {

		OutdegreeDatasetModel model = new OutdegreeDatasetModel();
		model.calc();
		return model.result.toString();

	}

	/**
	 * Dead datasetslinks
	 */

	@RequestMapping(value = "/linkset/valid/datasets/indegree", produces = MediaType.APPLICATION_JSON_VALUE)
	public String deadIndegree(@RequestParam(value = "n", required = false, defaultValue = "0") int n) {

		IndegreeDatasetModel model = new IndegreeDatasetModel();
		model.calc();
		return model.result.toString();

	}

	@RequestMapping(value = "/linkset/dead/datasets/outdegree", produces = MediaType.APPLICATION_JSON_VALUE)
	public String deadoutdegree(@RequestParam(value = "n", required = false, defaultValue = "0") int n) {

		OutdegreeDatasetModel model = new OutdegreeDatasetModel();
		model.isDeadLinks = true;
		model.calc();
		return model.result.toString();
	}

}
