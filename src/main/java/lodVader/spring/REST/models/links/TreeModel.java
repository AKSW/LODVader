package lodVader.spring.REST.models.links;

import java.util.ArrayList;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.queries.DatasetQueries;

public class TreeModel {

	ArrayList<TreeLeafModel> tree = new ArrayList<TreeLeafModel>();

	public void makeTree(Boolean onlyLinkedDatasets) {
		
		
		ArrayList<DatasetDB> d = new ArrayList<DatasetDB>();
		
		if(onlyLinkedDatasets)
			d = new DatasetQueries().getDatasetsNotVocabWithLinks();
		else
			d = new DatasetQueries().getTopDatasetsNotVocab();
		
		for (DatasetDB dataset : d) {
			ArrayList<DatasetDB> parent_list = dataset.getSubsetsAsMongoDBObject();
			
			tree.add(new TreeLeafModel(dataset.getLODVaderID(), "#", dataset.getTitle()));
			
			for (DatasetDB parent : parent_list) {
				for(int p: parent.getParentDatasetID()){
					if(p>0){
						tree.add(new TreeLeafModel(parent.getLODVaderID(), String.valueOf(p), parent.getTitle()));
						break;
					}
				}
				
			}
		}
	}

	public ArrayList<TreeLeafModel> getTree() {
		return tree;
	}

	public void setTree(ArrayList<TreeLeafModel> tree) {
		this.tree = tree;
	}
	
}
