package lodVader.spring.REST.models;

public class RESTMsg {
	private String coreMsg;

	private String parserMsg;
	
	public String getCoreMsg() {
		return coreMsg;
	}

	public void setCoreMsg(String coreMsg) {
		this.coreMsg = coreMsg;
	}

	public String getParserMsg() {
		return parserMsg;
	}

	public void setParserMsg(String parserMsg) {
		this.parserMsg = parserMsg;
	}
	
	public void setCoreMsgSuccess(){
		setCoreMsg("API successfully initialized.");
	}
}
