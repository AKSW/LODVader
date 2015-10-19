package lodVader.utils;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

import lodVader.LODVaderProperties;
import lodVader.Manager;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;

public class FileUtils {

	final static Logger logger = Logger.getLogger(FileUtils.class);

	public static void checkIfFolderExists() {

		// check if folders needed exists
		File f = new File(LODVaderProperties.BASE_PATH);
		if (!f.exists())
			f.mkdirs();

		f = new File(LODVaderProperties.FILTER_PATH);
		if (!f.exists())
			f.mkdirs();

		f = new File(LODVaderProperties.SUBJECT_PATH);
		if (!f.exists())
			f.mkdirs();

		f = new File(LODVaderProperties.OBJECT_PATH);
		if (!f.exists())
			f.mkdirs();

		f = new File(LODVaderProperties.FILE_URL_PATH);
		if (!f.exists())
			f.mkdirs();

		f = new File(LODVaderProperties.DUMP_PATH);
		if (!f.exists())
			f.mkdirs();
	}

	// TODO make this method more precise
	public static boolean acceptedFormats(String fileName)
			throws LODVaderFormatNotAcceptedException {

		if (fileName.contains(".ttl"))
			return true;
		else if (fileName.contains(".nt"))
			return true;
		else if (fileName.contains(".rdf"))
			return true;
		else if (fileName.contains(".zip"))
			return true;
		else if (fileName.contains(".bzip"))
			return true;
		else if (fileName.contains(".tgz"))
			return true;
		else if (fileName.contains(".gz"))
			return true;
		else {
			// throw new
			// DynamicLODFormatNotAcceptedException("File format not accepted: "
			// + fileName);
			return true;
		}
	}

	public static String stringToHash(String str) {
		String original = str;
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
			md.update(original.getBytes());
			byte[] digest = md.digest();
			StringBuffer sb = new StringBuffer();
			for (byte b : digest) {
				sb.append(String.format("%02x", b & 0xff));
			}

			logger.debug("Creating hash name for:" + original);
			logger.debug("digested(hex):" + sb.toString());
			return sb.toString();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void removeFile(String filePath) {
		try {
			File file = new File(filePath);
			file.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
