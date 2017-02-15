package stopwords;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StopWordsCountCombinerCompression extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new StopWordsCountCombinerCompression(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf());
      job.setNumReduceTasks(50);
      job.getConfiguration().setBoolean("mapred.compress.map.output", true);
      job.getConfiguration().setClass("mapred.map.output.compression.codec",
    		  BZip2Codec.class, CompressionCodec.class);
      
      job.setJarByClass(StopWordsCountCombinerCompression.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setCombinerClass(Combine.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

     
      
      FileInputFormat.addInputPath(job, new Path("input")); 
      Path outputPath = new Path("outputStopWordsCountCompression");
      FileOutputFormat.setOutputPath(job, outputPath);
      FileSystem hdfs = FileSystem.get(getConf());
	  if (hdfs.exists(outputPath))
	      hdfs.delete(outputPath, true);

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	      private final static IntWritable ONE = new IntWritable(1);
	      private Text word = new Text();

	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	         for (String token: value.toString().split("\\s+")) {
	        	token = token.replaceAll("[^a-zA-Z ]", "").toLowerCase();
	            word.set(token);
	            context.write(word, ONE);
	         }
	      }
	   }

	   
	   public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
		      @Override
		      public void reduce(Text key, Iterable<IntWritable> values, Context context)
		              throws IOException, InterruptedException {
		
		         int sum = 0;
		         for (IntWritable val : values) {
		            sum += 1;
		         }
		         
		         context.write(key, new IntWritable(sum));
		         	//System.out.println("combine text : " + key + " value : "+ sum);
		      }
		   }
	   
	   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	      @Override
	      public void reduce(Text key, Iterable<IntWritable> values, Context context)
	              throws IOException, InterruptedException {
	    	 
	         int sum = 0;
	         for (IntWritable val : values) {
	            sum += val.get();
	         }

	         System.out.println("reduce text : " + key + " value : "+ sum);	
	         
	         if(sum>4000){
	        	 context.write(key, new IntWritable(sum));
	         }
	         
	      }
	   }
}
