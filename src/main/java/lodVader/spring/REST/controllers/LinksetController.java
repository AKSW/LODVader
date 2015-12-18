package lodVader.spring.REST.controllers;

import java.util.ArrayList;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lodVader.API.diagram.Diagram;
import lodVader.spring.REST.models.links.DiagramModel;
import lodVader.spring.REST.models.links.TreeLeafModel;
import lodVader.spring.REST.models.links.TreeModel;

@RestController
public class LinksetController {

	@RequestMapping(value = "/linkset/graph")
	public Diagram list(
			@RequestParam(value = "dataset", required = true) Integer[] datasets,
			@RequestParam(value = "linkType", required = true) String type,
			@RequestParam(value = "showOntologies", required = false, defaultValue = "false") Boolean showOntologies,
			@RequestParam(value = "linkFrom", required = true) String min,
			@RequestParam(value = "linkTo", required = true) String max
			) {
		
		DiagramModel diagram = new DiagramModel(showOntologies, Double.parseDouble(min), Double.parseDouble(max), type, datasets);
		diagram.manageRequest();
		return diagram.diagramTemp;
	}
	
	@RequestMapping(value = "/linkset/tree")
	public ArrayList<TreeLeafModel> tree(
			@RequestParam(value = "linkedDatasets", required = false, defaultValue = "true") Boolean linkedDatasets
			) {
		
		TreeModel treeModel = new TreeModel();
		treeModel.makeTree(linkedDatasets);
		return treeModel.getTree();
	}
	
}
