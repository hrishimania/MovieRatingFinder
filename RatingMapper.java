package edu.tamu.isys.ratings;
/*
 * This is mapper class for Map-Reduce program. Mapper parses input record 
 * into Key-Value pair which acts as input for combiner class. We are implementing 
 * Key = "<Genre>" and Value = "<Movie_name>%t&<Rating>".
 * We are using "%t&" as separator between movie name and Rating which will be used by 
 * Combiner to split the value .
 * Some movies might have multiple genres associated with them, in this case we will create separate 
 * record for each genre. Erroneous records will be skipped and no Key-Value pair will
 * be generated for them.
 * 
 * @author Hrishikesh Shirsikar
 * @version 1.0
 * @Date: 11/3/2015
 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.tamu.isys.ratings.RatingParse;

public class RatingMapper extends
Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String data = value.toString();
		RatingParse entry = new RatingParse(data);
		 
		if (!entry.hasError()) {
 
			String[] genre = entry.getM_genre().split(",");	
			String tempM_name = entry.getM_name();
			String tempRating = entry.getRatings();			
			String stringValue = tempM_name.concat("%t&").concat(tempRating);
			Text newValue = new Text(stringValue) ;

			for(int i = 0; i < genre.length; i++ )
			{
				Text newKey = new Text(genre[i]);
				context.write(newKey, newValue);
			}							
		}		
	}
}
