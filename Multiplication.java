import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiplication {
	
	
	
	public static class MutiplicationMapper extends Mapper<Object, Text, Text, DoubleWritable>{
		Map<Integer, List<MovieRelation>> mapRelation = new HashMap<>();
		Map<Integer, Integer> totalWeight = new HashMap<>();
		
		@Override
		public void setup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			String comatrix = conf.get("comatrix");
			Path path = new Path(comatrix);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			//Input 10001:10004 4
			String line = br.readLine();
			while(line != null){
				String[] tokens = line.trim().split("\t");
				int movieA = Integer.parseInt(tokens[0].split(":")[0]);
				int movieB = Integer.parseInt(tokens[0].split(":")[1]);
				int relation = Integer.parseInt(tokens[1]);
				MovieRelation movie = new MovieRelation(movieA, movieB, relation);
				if(mapRelation.containsKey(movieA)){
					mapRelation.get(movieA).add(movie);
				}
				else{
					List<MovieRelation> list = new ArrayList<>();
					list.add(movie);
					mapRelation.put(movieA, list);
				}
				
				line = br.readLine();
			}
			br.close();
			
			for(Map.Entry<Integer, List<MovieRelation>> entry : mapRelation.entrySet()){
				int sum = 0;
				for(MovieRelation movie : entry.getValue()){
					sum += movie.getRelation();
				}
				totalWeight.put(entry.getKey(), sum);
			}
		}
			
		//input data: 1,10001,5 (userId, movieId, rating)
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
				
			String[] tokenStrings = value.toString().trim().split(",");
			int userId = Integer.parseInt(tokenStrings[0]);
			int movieId = Integer.parseInt(tokenStrings[1]);
			double rating = Double.parseDouble(tokenStrings[2]);
			
			for(MovieRelation movieRelation : mapRelation.get(movieId)){
				int movieB = movieRelation.getMovieB();
				double outputRating = movieRelation.getRelation() * rating;
				outputRating = outputRating / totalWeight.get(movieRelation.getMovieB());
				DecimalFormat df = new DecimalFormat("#.00");
				outputRating = Double.parseDouble(df.format(outputRating));
				context.write(new Text(userId + ":" + movieB), new DoubleWritable(outputRating));
			}
		}
	}
		
	public static class MutiplicationReducer extends Reducer<Text, DoubleWritable, IntWritable, Text>{
		
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double sum = 0;
			while (values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}
			DecimalFormat dFormat = new DecimalFormat("#.00");
			sum = Double.parseDouble(dFormat.format(sum));
			String[] tokenStrings = key.toString().trim().split(":");
			int userId = Integer.parseInt(tokenStrings[0]);
			context.write(new IntWritable(userId), new Text(tokenStrings[1] + ":" +sum));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("comatrix", args[0]);
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MutiplicationMapper.class);
		job.setReducerClass(MutiplicationReducer.class);
		job.setJarByClass(Multiplication.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}

}
