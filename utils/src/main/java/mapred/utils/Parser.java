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
	
	/**
	 * Parses the String line and produces Category and Price data
	 * @param line String
	 * @return this Parser
	 */
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
	
	/**
	 * returns the last parsed category
	 * @return category String
	 */
	public String getCat() {
		return cat;
	}
	
	/**
	 * returns the last parsed price
	 * @return price Double
	 */
	public Double getVal() {
		return val; 
	}
	
	/**
	 * Returns true if an only if both cat and val are non-null
	 * else returns false
	 * @return isValid boolean
	 */
	public boolean isValid() {
		return val != null && cat != null;
	}
}