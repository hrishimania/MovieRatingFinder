package edu.tamu.isys.ratings;
/*
 * This is Combiner class for Map-Reduce program. Combiner will use Key-Value 
 * pairs form mapper where Key = "<Genre>" and Value = "<Movie_name>%t&<Rating>".
 * Values will be split at string "%t&" and hashmap 'Map' will be created with 
 * structure being (Key = <movie_name>,Value = <rating, rating,....>). 
 * Using this 'Map', average rating for each movie will be calculated and second 
 * hashmap 'MapOne' will be created with structure (Key = <movie_name>,Value = <rating>).
 * Output of the Combiner will be Key-Value pairs for Reducer and we are implementing
 * Key = "<Genre>" and Value = "<Movie_name>%t&<Average Rating>". Here string "%t&"
 * is used as separator.
 * 
 * @author Hrishikesh Shirsikar
 * @version 1.0
 * @Date: 11/3/2015
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingCombiner extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, ArrayList<Double>> map = getMap(values);
		Map<String, Double> mapOne = getMapOne(map);

		for(Entry<String, Double> entry : mapOne.entrySet()){

			String mapKey = entry.getKey();
			Double mapValue = entry.getValue();
			String stringValue = mapValue.toString();
			String mapEntry = mapKey.concat("%t&").concat(stringValue);
			Text newValue = new Text(mapEntry) ;
			context.write(key, newValue);
		}

	}

	private Map<String, ArrayList<Double>> getMap(Iterable<Text> values) {
		Map<String, ArrayList<Double>> map = new HashMap<String, ArrayList<Double>>();
		for (Text value : values) {

			String stringValue = value.toString();
			String[] chunks = stringValue.split("%t&");

			String m_name = chunks[0];
			Double rating = Double.parseDouble(chunks[1]);
			try {

				if (map.get(m_name) == null){
					map.put(m_name, new ArrayList<Double>());
				}

				map.get(m_name).add(rating);

			} catch(Exception e) {
				e.printStackTrace();
			}			
		}		
		return map;
	}

	private Map<String, Double> getMapOne(Map<String, ArrayList<Double>> map){
		Map<String, Double> mapOne = new HashMap<String, Double>();

		for (Entry<String, ArrayList<Double>> entry : map.entrySet()) {
			String key = entry.getKey();
			ArrayList<Double> values = entry.getValue();

			int length = values.size();
			double sum = 0d;

			for(int i = 0; i < length; i++){
				sum += values.get(i);
			}

			double avgRating = sum / length;

			if(mapOne.get(key) == null){
				mapOne.put(key, avgRating);
			}
		}

		return mapOne;
	}

}
