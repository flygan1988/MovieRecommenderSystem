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

/*
 * Take the output of the first job as the input 
 * Input data: 1	10001:4.0,10003:3.0,10007:5.0
 * Output data like this: 10001:10003	3
 */
public class GenerateCoMatrix {
	
	public static class GenerateCoMatrixMapper extends Mapper<Object, Text, Text, IntWritable>{
			
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			String str = value.toString();
			String[] user_movie = str.split("\t");
			String[] movies = user_movie[1].split(",");
			
			for(int i = 0; i < movies.length; i++){
				String movieA = movies[i].trim().split(":")[0];
				for (int j = 0; j < movies.length; j++) {
					String movieB = movies[j].trim().split(":")[0];
					context.write(new Text(movieA + ":" + movieB), new IntWritable(1));
				}
			}
		}
	}
		
	public static class GenerateCoMatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
			
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			while (values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Generating");
		
		job2.setMapperClass(GenerateCoMatrix.GenerateCoMatrixMapper.class);
		job2.setReducerClass(GenerateCoMatrix.GenerateCoMatrixReducer.class);
		job2.setJarByClass(GenerateCoMatrix.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job2, new Path(args[0]));
		TextOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		job2.waitForCompletion(true);
	}

}
