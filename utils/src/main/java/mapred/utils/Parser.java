package mapred.utils;
/**
 * Simple parser specific for a1 and a2
 * parses the valid entry of category and price
 * into data variables named cat and val respectively
 * @author nikit
 *
 */
public class Parser {
	private String cat;
	private Double val;
	
	public Parser  parse(String line) {
		String parts[] = line.trim().split("\t");
		if(parts.length == 6) {
			
			cat = parts[3];
			try {
				val = Double.parseDouble(parts[4]);
			} catch(Exception e) {
				// do nothing
			}
		} else {
			cat = null;
			val = null;
		}
		return this;
	}
	
	public String getCat() {
		return cat;
	}
	
	public Double getVal() {
		return val; 
	}
	
	public boolean isValid() {
		return val != null && cat != null;
	}
}