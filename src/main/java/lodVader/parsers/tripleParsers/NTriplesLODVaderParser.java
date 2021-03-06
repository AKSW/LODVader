package lodVader.parsers.tripleParsers;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.atomic.AtomicBoolean;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.helpers.RDFParserBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.tupleManager.SuperTupleManager;

public class NTriplesLODVaderParser extends RDFParserBase {

	final static Logger logger = LoggerFactory.getLogger(RDFParserBase.class);
	AtomicBoolean startedStreaming = new AtomicBoolean(false);

	BlockingQueue<String> bufferQueue = new ArrayBlockingQueue<String>(100000);
	volatile boolean doneReading = false;

	// private int BUFFER_SIZE = 655360;
	private int BUFFER_SIZE = 1024 * 8;

	static int numberOfReadedResources = 0;

	public void stream(InputStream inStream) {

		final InputStream inputStream = inStream;

		try {
			Thread t = new Thread(new Runnable() {

				public void run() {
					Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
					try {

						int nRead;
						byte[] data = new byte[BUFFER_SIZE];
						int sleeping = 0;

						while ((nRead = new BufferedInputStream(inputStream).read(data, 0, data.length)) != -1) {
							startedStreaming.set(true);
							// bufferQueue.add(new String(data,
							// StandardCharsets.UTF_8));
							// if (++numberOfReadedResources % 1000 == 0) {
							// System.out.println(numberOfReadedResources);
							// }
							try {
								bufferQueue.put(new String(data, 0, nRead, StandardCharsets.UTF_8));
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							// while (bufferQueue.size() > 500000) {
							// Thread.sleep(3);
							// if (sleeping % 5000 == 0)
							// System.out.println("Streaming thread is
							// sleeping...");
							// sleeping++;
							// }
						}
						startedStreaming.set(true);
						doneReading = true;
					} catch (IOException e) {
						doneReading = true;
					}
					// catch (InterruptedException e) {
					// e.printStackTrace();
					// doneReading = true;
					// }
					logger.info("DONE STREAMING!!!");
				}
			});
			t.setName("StreammingThread");
			t.start();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	protected void parse() {

		SuperTupleManager splitAndStore = (SuperTupleManager) getRDFHandler();

		try {

			String subjectStmt = "";
			String propertyStmt = "";
			String objectStmt = "";

			String lastLine = "";

			int showMsgInterval = 4;
			int bufferCount = -1;
			// block this thread is the streaming process did not started yet
			while(!startedStreaming.get())
				Thread.sleep(5);

			while (bufferQueue.size() > 0 || !doneReading) {
				bufferCount++;

				// shows buffer size each interval
				if (bufferCount % showMsgInterval == 0)
					logger.debug("Buffer Streaming size: " + bufferQueue.size());

				try {
					// split queue line by line
					String triples[];
					triples = bufferQueue.take().split("\n");

					// case buffer starts with an incomplete triple,
					// concatenate with the last line of the previous buffer
					if (!lastLine.equals("")) {
						triples[0] = lastLine.concat(triples[0]);
						lastLine = "";
					}

					// for each triple, separate s, p, o
					for (int q = 0; q < triples.length; q++) {
						String triple = triples[q];

						// if (!triple.startsWith("#")) {
						try {
							// Pattern pattern = Pattern
							// .compile("^(<[^>]+>)\\s+(<[^>]+>)\\s(.*)(\\s\\.)");

							// Pattern pattern = Pattern
							// .compile("^<([^>]+)>\\s+<([^>]+)>\\s<(.*)>(\\s\\.)");

							Pattern pattern = Pattern.compile("^<([^>]+)>\\s+<([^>]+)>\\s(.*)(\\s\\.)");

							Matcher matcher = pattern.matcher(triple);

							// case it is an incomplete line means it is the
							// end of the buffer
							// handle in the catch statement
							if (!matcher.matches()) {
								throw new ArrayIndexOutOfBoundsException();
							}

							subjectStmt = matcher.group(1);
							propertyStmt = matcher.group(2);
							objectStmt = matcher.group(3);
							if (objectStmt.contains(" "))
								if (objectStmt.contains("> <"))
									objectStmt = objectStmt.split(" ")[0].split(">")[0];

							splitAndStore.saveStatement(subjectStmt, propertyStmt, objectStmt);

						} catch (ArrayIndexOutOfBoundsException e) {
							lastLine = triple;
						}
						// }
					}

				} catch (NoSuchElementException em) {
					// em.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

		}

	}

	@Override
	public RDFFormat getRDFFormat() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void parse(InputStream in, String baseURI) throws IOException, RDFParseException, RDFHandlerException {
		stream(in);
		parse();

	}

	@Override
	public void parse(Reader reader, String baseURI) throws IOException, RDFParseException, RDFHandlerException {
		// TODO Auto-generated method stub

	}

}
