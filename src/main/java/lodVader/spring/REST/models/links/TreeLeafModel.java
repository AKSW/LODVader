package lodVader.spring.REST.models.links;

public class TreeLeafModel {
	
	private int id;

	private String parent;

	private String text;

	public TreeLeafModel(Integer id, String parent, String text) {
		this.id = id;
		this.parent = parent;
		this.text = text;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getParent() {
		return parent;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
}
