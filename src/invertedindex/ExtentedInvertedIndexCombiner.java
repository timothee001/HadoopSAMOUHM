package invertedindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExtentedInvertedIndexCombiner extends Configured implements Tool {
	
	
	
   public static void main(String[] args) throws Exception {
     // System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new ExtentedInvertedIndexCombiner(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(),"ExtentedInvertedIndexCombiner");
      job.setJarByClass(ExtentedInvertedIndexCombiner.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setCombinerClass(Combine.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path("input")); 
      Path outputPath = new Path("outputExtentedInvertedIndexCombiner");
      FileOutputFormat.setOutputPath(job, outputPath);
      FileSystem hdfs = FileSystem.get(getConf());
	  if (hdfs.exists(outputPath))
	      hdfs.delete(outputPath, true);

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
     
      private Text word = new Text();
      ArrayList<String> allstopwords = ReadCSV.getStopWords();
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  Path filePath = ((FileSplit) context.getInputSplit()).getPath();
    	  String filename = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
    	 
         for (String token: value.toString().split("\\s+")) {
        	 //System.out.println(token);
        	 token = token.replaceAll("[^a-zA-Z ]", "").toLowerCase();
        	if(!allstopwords.contains(token)){
        		word.set(token);
                context.write(word, new Text(filename));
        	}        
         }
      }
   }

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
        //System.out.println("Key : " + key.toString());
    	// System.out.println("Reduce");
        
    	 ArrayList<String> doccount = new ArrayList<String>();
         for (Text val : values) {
        	//System.out.println(val);
        	 
        		 doccount.add(val.toString()); 
        		 //System.out.println(val.toString());
        	
        	
         }
         
         //System.out.println(key);
         //System.out.println(docnames.toString());
         String textToWrite = doccount.toString().replace("[", "").replace("]", "").replace("{", "").replace("}", "").replace("=", "#");
         context.write(key, new Text(textToWrite));
      }
   }
   
   public static class Combine extends Reducer<Text, Text, Text, Text> {
	      @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	        //System.out.println("Key : " + key.toString());
	    	 // System.out.println("Combine");
	        
	    	
	    	 HashMap<String,Integer> doccount = new HashMap<String,Integer>();
	         for (Text val : values) {
	        	//System.out.println(val);
	        	 if(doccount.containsKey(val.toString())){
	        		 doccount.put(val.toString(), doccount.get(val.toString())+1); 
	        		// System.out.println(val.toString());
	        	 }else{
	        		 doccount.put(val.toString(), 1) ; 
	        		 //System.out.println(val.toString());
	        	 } 
	        	
	         }
	         
	         //System.out.println(key);
	         //System.out.println(docnames.toString());
	         context.write(key, new Text(doccount.toString()));
	      }
	   }
}
