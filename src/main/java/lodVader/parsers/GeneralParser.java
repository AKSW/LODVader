package lodVader.parsers;

import java.io.InputStream;

import org.openrdf.rio.RDFParser;

public interface GeneralParser extends RDFParser{

	public void parse(InputStream in, String URI, int limit);

}
