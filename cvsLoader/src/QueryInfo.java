import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class QueryInfo {
	private String filename;
	private String[] queries;
	private String year;
	private int type;
	
	public QueryInfo(String filename, String[] queries) throws Exception {
		this.filename = filename;
		this.queries = queries;
		this.year = estractYear(filename);
		
		if(queries.length == 1) {
			this.type = 1;
		} else {
			this.type = 2;
		}
	}

	
	public int getType() {
		return type;
	}


	public void setType(int type) {
		this.type = type;
	}


	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String[] getQueries() {
		return queries;
	}

	public void setQueries(String[] queries) {
		this.queries = queries;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	private String estractYear(String filename) throws Exception {
		 Pattern findUrl = Pattern.compile("(19|20)\\d{2}");
		 Matcher matcher = findUrl.matcher(filename);
		 
		 if(matcher.find()) {
		   return matcher.group();
		 } else {
			 throw new Exception("Data non trovata!");
		 }
	}
		
}
