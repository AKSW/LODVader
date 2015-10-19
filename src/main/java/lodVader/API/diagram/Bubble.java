package lodVader.API.diagram;

import org.json.JSONObject;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;

public class Bubble {

	Object dynLodObject;

	int id;

	String text;

	String name;

	String color;
	
	boolean visible = false; 
	
	int radius;
	
	int group;
	
	boolean isVocab = false;

	
	
	public JSONObject getJSON() {
		JSONObject node = new JSONObject();

		node.put("text", getText());
		node.put("group", group);
		DatasetDB d = new DatasetDB(group);
		
		if(!d.getTitle().equals(""))
			node.put("group_name", d.getTitle());
		else if(!d.getLabel().equals(""))
			node.put("group_name", d.getLabel());
		else
			node.put("group_name", group);		
		node.put("color", getColor());
		node.put("name", getID());
		node.put("radius", getRadius());
		node.put("isVocab", isVocab);
		

		return node;
	}

	public Bubble(Object source, boolean visible) {
		this.visible = visible;
		startBubble(source);
	}
	
	public Bubble(Object source, boolean visible, int group) {
		this.visible = visible;
		this.group = group;
		startBubble(source);
	}
	
	public Bubble(Object source) {
		startBubble(source);
	}
	
	private void startBubble(Object source){
		if (source instanceof DistributionDB) {

			DistributionDB tmp = (DistributionDB) source;
			this.group = tmp.getTopDataset();
			this.isVocab = tmp.getIsVocabulary();
			if (tmp.getTitle() != null && !tmp.getTitle().equals(""))
				setText(tmp.getTitle());
			else
				setText(tmp.getUri());
			setName(tmp.getDownloadUrl());
			setID(tmp.getLODVaderID());
	
				setRadius(31);


			if (tmp.getIsVocabulary()){
				setColor("rgb(253, 174, 107)");
				setRadius(30);
			}
			else
				setColor("rgb(66, 136, 78)");

			dynLodObject = (DistributionDB) source;
		}

		else if (source instanceof DatasetDB) {
			DatasetDB tmp = (DatasetDB) source;
			this.isVocab = tmp.getIsVocabulary();

			if (tmp.getTitle() != null || !tmp.getTitle().equals(""))
				setText(tmp.getTitle());
			else if (tmp.getLabel() != null || !tmp.getLabel().equals(""))
				setText(tmp.getLabel());
			else
				setText(tmp.getUri());
			
			setName(tmp.getUri());
			setID(tmp.getLODVaderID());
		

			setRadius(31);

			if (tmp.getIsVocabulary()){
				setColor("rgb(253, 174, 107)");
				setRadius(27);
			}
			else
			setColor("rgb(116, 196, 118)");

			dynLodObject = (DatasetDB) source;
		}
	}

	public String getText() {

		setText(text.split("@")[0]);
		setText(text.split("http")[0]);

		if (text.length() > 145) {
			setText(text.substring(0, 145) + "...");
		}
		return text;
//		return String.valueOf(isVisible());
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getColor() {
		return color;
	}

	public void setColor(String color) {
		this.color = color;
	}

	public int getID() {
		return id;
	}

	public void setID(int id) {
		this.id = id;
	}

	public int getRadius() {
		return radius;
	}

	public void setRadius(int radius) {
		this.radius = radius;
	}

	public boolean isVisible() {
		return visible;
	}

	public void setVisible(boolean visible) {
		this.visible = visible;
	}
	
	
	

}
