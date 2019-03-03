package lodVader.spring.measures;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;
import java.util.TreeSet;

import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;

import lodVader.enumerators.TuplePart;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.collections.LinksAssessment;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import lodVader.mongodb.queries.PredicatesQueries;

public class TestLinks {

	TreeSet<Integer> randoms = new TreeSet<Integer>();

	TuplePart tuplePart = TuplePart.PROPERTY;

	public void makeRandom() {
		Random r = new Random();
		int Low = 1;
		// subjects = 195000000
		// subjects no bio
		// objects 94000
		// objects no bio 94000
		// properties 74814
		

		int High = 74814;
		for (int i = 0; i < 9999; i++) {
			int o = (r.nextInt(High - Low) + Low);
			randoms.add(o);
		}
	}

	public TestLinks() {
		String path;

		if (tuplePart.equals(tuplePart.SUBJECT))
			path = "/home/ciro/dataid/ISWCData/subjects_no_bio";
		else if (tuplePart.equals(tuplePart.OBJECT))
			path = "/home/ciro/dataid/ISWCData/objects_no_bio";
		else if (tuplePart.equals(tuplePart.PROPERTY)){
			path = "/home/ciro/dataid/ISWCData/properties";
			try {
				File file = new File(path);
				if (!file.exists()) {
					BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)));
					for (AllPredicatesDB all : new PredicatesQueries().getAllPredicatesRegex("")) {
						bw.write(all.getUri() + "\n");
					}
					bw.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace(); 
			}
		}
		else{
			path =  "/home/ciro/dataid/ISWCData/invalid";
		}

		try {
			makeSample(path);
			test(path);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void makeSample(String path) throws Exception {
		makeRandom();
		FileInputStream fis = new FileInputStream(new File(path));

		File file = new File(path + "_sample");

		if (file.exists())
			return;

		BufferedWriter bw = new BufferedWriter(new FileWriter(file));

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		int i = 1;
		while ((line = br.readLine()) != null) {
			if (randoms.contains(i))
				bw.write(line + "\n");
			i++;
		}

		br.close();
		bw.close();
	}

	public void test(String path) throws Exception {
		FileInputStream fis = new FileInputStream(new File(path + "_sample"));

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		int i = 1;
		while ((line = br.readLine()) != null) {

			final String link = line;

			LinksAssessment l = new LinksAssessment();
			l.setLink(link);

			if (l.find(true)) {
				System.out.println("Already saved");
			} else {
				Thread.sleep(150);

				// System.out.println(line);
				Thread t = new Thread(new Runnable() {
					public void run() {
						int responseCode = 0;

						HttpURLConnection connection;

						try {
							LinksAssessment l;

							System.out.println();

							System.out.println(link);

							l = new LinksAssessment();
							l.setLink(link);

							if (l.find(true)) {
								System.out.println("Already saved");
								return;
							}

							connection = (HttpURLConnection) new URL(link).openConnection();
							connection.setRequestProperty("Accept", "application/rdf+xml");
							connection.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
							connection.addRequestProperty("User-Agent", "Mozilla");
							connection.addRequestProperty("Referer", "google.com");
							responseCode = connection.getResponseCode();
							System.out.println("Response Code ... " + responseCode);

							boolean redirect = false;

							if (responseCode != HttpURLConnection.HTTP_OK) {
								if (responseCode == HttpURLConnection.HTTP_MOVED_TEMP
										|| responseCode == HttpURLConnection.HTTP_MOVED_PERM
										|| responseCode == HttpURLConnection.HTTP_SEE_OTHER)
									redirect = true;
							}

							if (redirect) {

								// get redirect url from "location" header field
								String newUrl = connection.getHeaderField("Location");

								// get the cookie if need, for login
								String cookies = connection.getHeaderField("Set-Cookie");

								// open the new connnection again
								connection = (HttpURLConnection) new URL(newUrl).openConnection();
								connection.setRequestProperty("Cookie", cookies);
								connection.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
								connection.addRequestProperty("User-Agent", "Mozilla");
								connection.addRequestProperty("Referer", "google.com");
								connection.setRequestProperty("Accept", "application/rdf+xml");

								responseCode = connection.getResponseCode();

								System.out.println("Redirect to URL : " + newUrl);
								System.out.println("New Response Code ... " + responseCode);

							}

							try {
								InputStream response = connection.getInputStream();

								System.out.println(connection.getContentType());

								if (connection.getContentType().contains("rdf")) {
									l = new LinksAssessment(link, "application/rdf+xml", tuplePart,
											connection.getResponseCode(), "");

								} else {
									l = new LinksAssessment(link, connection.getContentType(), tuplePart, responseCode,
											"");

								}
							} catch (IOException ei) {
								l = new LinksAssessment(link, "", tuplePart, responseCode, ei.getMessage());
							}

							l.update(true);

						}

						catch (Exception e2) {
							LinksAssessment l = new LinksAssessment(link, "", tuplePart, responseCode, e2.getMessage());
							e2.printStackTrace();
							try {
								l.update(true);
							} catch (LODVaderMissingPropertiesException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (LODVaderObjectAlreadyExistsException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (LODVaderNoPKFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				});

				t.start();
			}
		}

		br.close();
	}

}
