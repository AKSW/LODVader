package lodVader.streaming;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.DecimalFormat;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import lodVader.exceptions.LODVaderLODGeneralException;

public class Stream {

	final static Logger logger = Logger.getLogger(Stream.class);

	// HTTP header fields
	public String httpDisposition = null;
	public String httpContentType = null;
	public double httpContentLength;
	public String httpLastModified = "0";

	protected static final int BUFFER_SIZE = 1024*256;
	public URL url = null;

	protected InputStream inputStream = null;

	final byte[] buffer = new byte[BUFFER_SIZE];
	int n = 0;
	int aux = 0;

	public String fileName = null;
	public String extension = null;
	public String RDFFormat = null;

	HttpURLConnection httpConn = null;

	String accessURL = null;

	protected void getMetadataFromHTTPHeaders(HttpURLConnection httpConn) {

		httpDisposition = httpConn.getHeaderField("Content-Disposition");
		httpContentType = httpConn.getContentType();
		httpContentLength = httpConn.getContentLength();
		if (httpConn.getLastModified() > 0)
			httpLastModified = String.valueOf(httpConn.getLastModified());

		printHeaders();

	}

	protected void openStream() throws IOException, LODVaderLODGeneralException   {
		openConnection();

		// opens input stream from HTTP connection
		InputStream inputStream = new BufferedInputStream(httpConn.getInputStream());

		logger.debug("InputStream from http connection opened");

		// get some data from headers
		getMetadataFromHTTPHeaders(httpConn);

		this.inputStream = inputStream;

	}

	private void openConnection() throws IOException, LODVaderLODGeneralException  {
		httpConn = (HttpURLConnection) url.openConnection();

		httpConn.setReadTimeout(5000);
		httpConn.setConnectTimeout(5000);
		int responseCode = httpConn.getResponseCode();

		logger.debug("Open HTTP connection for URL: " + url.toString());

		// check HTTP response code
		if (responseCode != HttpURLConnection.HTTP_OK) {
			httpConn.disconnect();
			throw new LODVaderLODGeneralException (
					"No file to download. Server replied HTTP code: "
							+ responseCode);
		}
		logger.debug("Successfuly connected with HTTP OK status.");

	}

	protected void printHeaders() {
		DecimalFormat df = new DecimalFormat("#.##");

		logger.debug("Content-Type = " + httpContentType);
		logger.debug("Last-Modified = " + httpLastModified);
		logger.debug("Content-Disposition = " + httpDisposition);
		logger.debug("Content-Length = "
				+ df.format(httpContentLength / 1024 / 1024) + " MB");
		logger.debug("fileName = " + fileName);
	}

	protected void checkBZip2InputStream() throws IOException  {

		// check whether file is bz2 type
		if (getExtension().equals("bz2")) {
			logger.info("File extension is bz2, creating BZip2CompressorInputStream...");
			httpConn = (HttpURLConnection) url.openConnection();
			inputStream = new BZip2CompressorInputStream(new BufferedInputStream(
					httpConn.getInputStream()), true);
			setFileName(getFileName().replace(".bz2", ""));
			setExtension(null);

			logger.info("Done creating BZip2CompressorInputStream! New file name is "
					+ getFileName());
		}
	}

	protected void checkGZipInputStream() throws IOException {

		// check whether file is gz type
		if (getExtension().equals("gz") || getExtension().equals("tgz")) {
			logger.info("File extension is " + getExtension()
					+ ", creating GzipCompressorInputStream...");
			logger.debug(new FileNameFromURL().getFileName(
					url.toString(), httpDisposition));
			httpConn = (HttpURLConnection) url.openConnection();
			inputStream = new GzipCompressorInputStream(
					new BufferedInputStream(httpConn.getInputStream()), true);
			setFileName(getFileName().replace(".gz", ""));
			setFileName(getFileName().replace(".tgz", ".tar"));
			if (getFileName().contains(".tar"))
				setExtension("tar");
			if (getExtension().equals("tgz"))
				setExtension("tar");
			else
				setExtension(null);

			logger.info("Done creating GzipCompressorInputStream! New file name is "
					+ getFileName() + ", extension: "+getExtension());
		}
	}

	/**
	 * Get the file name of the current file being streamed
	 * @return file name
	 */
	public String getFileName() {
		if (fileName == null) {
			// extracts file name from header field
			fileName = new FileNameFromURL().getFileName(url.toString(),
					httpDisposition);
			logger.debug("Found file name: " + fileName);
		}
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getExtension() {
		if (extension == null) {
			logger.info("Setting file extension.");
			extension = FilenameUtils.getExtension(getFileName());
			logger.info(extension);
		}
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}

	public URL getUrl() {
		return url;
	}

	public void setUrl(URL url) {
		this.url = url;
	}

	/**
	 * Stream a file. 
	 * @param file the file name
	 * @param stream the inputStream
	 */
	public void simpleDownload(String file, InputStream stream) {
		try {
			ReadableByteChannel rbc = Channels.newChannel(stream);
			FileOutputStream fos = new FileOutputStream(file);
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
