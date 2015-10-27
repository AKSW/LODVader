package lodVader.utils;

import org.junit.Test;

public class NSUtils {

	/**
	 * Get namespace from a URL
	 * 
	 * @param url
	 *            the URL to be checked
	 * @return the namespace
	 */
	
//	@Test
//	public void getNSFromString() {
//		String url = "http://www.semanticdesktop.org/asciro/asd/asss/aaaa/bbb/ccc";
		public String getNSFromString(String url) {
		// if(url.endsWith("/"))
		// url = url.substring(0, url.length()-1);

		String[] split = url.split("/");
		
		if (split.length > 7) {
			if (split[7].contains("#"))
				url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/" + split[5] + "/" + split[6] + 	"/" + split[7].split("#")[0] + "#";
			else
				url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/" + split[5] + "/" + split[6] + 	"/";
		} 
		
		else if (split.length > 6){
			if (split[6].contains("#"))
				url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/" + split[5] + "/" + split[6].split("#")[0] + "#";
			else
				url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/" + split[5] + "/";
		}
		
		else if (split.length > 5){
			if (split[5].contains("#"))
				url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/" + split[5].split("#")[0] + "#";
			else
				url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/";		
		}
		
		else if (split.length > 4){
			if (split[4].contains("#"))
				url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4].split("#")[0] + "#";
			else
				url = split[0] + "//" + split[2] + "/" + split[3] + "/";
		}
		
		else if (split.length > 3){
			if (split[3].contains("#"))			
				url = split[0] + "//" + split[2] + "/" + split[3].split("#")[0] + "#";
			else				
				url = split[0] + "//" + split[2] + "/";
		}
		else {
			url = "";
		}
//		System.out.println(url);
		return url;
	}

}
