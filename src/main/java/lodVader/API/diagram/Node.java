package lodVader.API.diagram;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;

public class Node {

	Object dynLodObject;

	@JsonIgnore
	int id;

	String text;

	@JsonIgnore
	String url;

	String color;

	@JsonIgnore
	boolean visible = false;

	int radius;

	int group;

	boolean isVocab = false;

	public Node(Object source, boolean visible) {
		this.visible = visible;
		startBubble(source);
	}

	public Node(Object source, boolean visible, int group) {
		this.visible = visible;
		this.group = group;
		startBubble(source);
	}

	public Node(Object source) {
		startBubble(source);
	}

	private void startBubble(Object source) {
		if (source instanceof DistributionDB) {

			DistributionDB tmp = (DistributionDB) source;
			this.group = tmp.getTopDatasetID();
			this.isVocab = tmp.getIsVocabulary();
			if (tmp.getTitle() != null && !tmp.getTitle().equals(""))
				setText(tmp.getTitle());
			else
				setText(tmp.getUri());
			setUrl(tmp.getDownloadUrl());
			setID(tmp.getLODVaderID());

			setRadius(31);

			if (tmp.getIsVocabulary()) {
				setColor("rgb(253, 174, 107)");
				setRadius(30);
			} else
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

			setUrl(tmp.getUri());
			setID(tmp.getLODVaderID());

			setRadius(31);

			if (tmp.getIsVocabulary()) {
				setColor("rgb(253, 174, 107)");
				setRadius(27);
			} else
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
		// return String.valueOf(isVisible());
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getUrl() {
		return url;
	}
	
	public int getName() {
		return getID();
	}

	public String getGroup_name() {
		DatasetDB d = new DatasetDB(group);
		if (!d.getTitle().equals(""))
			return d.getTitle();
		else if (!d.getLabel().equals(""))
			return d.getLabel();
		else
			return String.valueOf(group);
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getColor() {
		return color;
	}
	
	public int getGroup() {
		return group;
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

	public void setVocab(boolean isVocab) {
		this.isVocab = isVocab;
	}
	
	public boolean getIsVocab(){
		return isVocab;
	}

}
