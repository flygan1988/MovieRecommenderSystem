import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//Input data format: 1,10001,5.0(userId,movieId,rating);
//We should output data: 1	<10001:5.0,10002:3.0...>
public class DataPreProcessor {
	
	public static class PreProcessMapper extends Mapper<Object, Text, IntWritable, Text>{
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
				String str = value.toString();
				String[] user_movie_rating = str.trim().split(",");
				int userId = Integer.parseInt(user_movie_rating[0]);
				String output = user_movie_rating[1] + ":" + user_movie_rating[2];
				context.write(new IntWritable(userId), new Text(output));
		}
	}
		
	public static class PreProcessReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
			
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
				
			StringBuilder sb = new StringBuilder();
			while(values.iterator().hasNext()){
				String value = values.iterator().next().toString().trim();
				sb.append(",");
				sb.append(value);
			}
			context.write(key, new Text(sb.toString().replaceFirst(",", "")));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PreProcessData");
		
		job.setMapperClass(PreProcessMapper.class);
		job.setReducerClass(PreProcessReducer.class);
		job.setJarByClass(DataPreProcessor.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
