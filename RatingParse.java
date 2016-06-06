package edu.tamu.isys.ratings;
/*
 * This class is used to define methods for parsing a record from input file.
 * A record from input will be split into chunks at "::" as it is used as separator 
 * in input file. In a ideal record movie name is at second position, genre at third and 
 * rating at seventh position. Variable Error, when set to a non-empty value, indicates 
 * that a record is erroneous. Error will be set to value "1" in case of record containing
 * ":::" or movie name, genre or rating being returned empty.
 *  
 * @author Hrishikesh Shirsikar
 * @version 1.0
 * @Date: 11/3/2015
 */
public class RatingParse {

	private String m_name = ""; //movie name
	private String m_genre = ""; //genre
	private String ratings = ""; 
	private String error = ""; 

	public RatingParse(String rawData) {
		try {

			if(rawData.contains(":::")){
				error = "1";
			}

			String[] chunks = rawData.split("::");
			
			this.m_name  = chunks[1];
			this.m_genre = chunks[2];
			this.ratings = chunks[6];		

			try {

				Double doubleRating = Double.parseDouble(ratings);
				
			} catch(Exception e) {
				error = "1";
				e.printStackTrace();
			}

			if(this.m_name.isEmpty()){
				error = "1";
			}
			if(this.m_genre.isEmpty()){
				error = "1";
			}
			if(this.ratings.isEmpty()){
				error = "1";
			}

		} catch(Exception e) {
			error = e.getStackTrace().toString() + "***" + rawData;
		}		
	}

	public String getM_name() {
		return m_name;
	}

	public String getM_genre() {
		return m_genre;
	}

	public String getRatings() {
		return ratings;
	}

	public String getError() {
		return error;
	}

	public boolean hasError() {
		return !error.isEmpty();
	}
}
