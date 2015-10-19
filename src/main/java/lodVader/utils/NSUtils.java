package lodVader.utils;

public class NSUtils {

	/**
	 * Get namespace from a URL
	 * @param url the URL to be checked
	 * @return the namespace
	 */
	public String getNSFromString(String url){
		String[] split = url.split("/");	
		if (split.length > 5)
			url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/"+ split[5] + "/";
		else if (split.length > 4)
			url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/";
		else if(split.length > 3)
			url = split[0] + "//" + split[2] + "/" + split[3] + "/";
		else if (split.length > 2)
			url = split[0] + "//" + split[2] + "/";
		else {
			url = "";
		}
		return url;
	}
	
}
