package edu.tamu.isys.ratings;

/*
 * This is Reducer class for Map-Reduce program. Reducer will use Key-Value pairs
 * from Combiner where Key = "<Genre>" and Value = "<Movie_name>%t&<Average Rating>".
 * Values will be split at string "%t&" and hashmap 'Map' will be created with 
 * structure being (Key = <movie_name>,Value = <average rating>). We will find movie 
 * with the highest average rating. This movie and its average rating is the required 
 * output for the Map-Reduce program. Output values are formatted to display Ratings 
 * value up to two decimals.
 * 
 * @author Hrishikesh Shirsikar
 * @version 1.0
 * @Date: 11/3/2015
 */
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, Double> map = getMap(values);
		String finalM = getFinalMovie(map);
		Text newValue = new Text(finalM.toString());
		context.write(key, newValue);
	}

	private Map<String,Double> getMap(Iterable<Text> values) {
		Map<String,Double> map = new HashMap<String, Double>();
		for (Text value : values) {

			String stringValue = value.toString();
			String[] chunks = stringValue.split("%t&");

			String m_name = chunks[0];
			try {
				Double avgRating = Double.parseDouble(chunks[1]);

				if (map.get(m_name) == null){
					map.put(m_name, avgRating);
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		return map;
	}

	private String getFinalMovie(Map<String, Double> map){

		String highestKey = "";
		Double highestValue = 0d;
		for(Entry<String, Double> entry : map.entrySet()){

			String key = entry.getKey();
			Double value = entry.getValue();

			if( highestKey.isEmpty())
			{
				highestKey = key;
				highestValue  = value;
			}
			else if (value > highestValue) {
				highestKey = key;
				highestValue  = value;				
			} 
		}

		DecimalFormat decFormat = new DecimalFormat("##.00");
		String finalM = highestKey.concat(" (").concat(decFormat.format(highestValue)).concat(")");
		return finalM;
	}

}
