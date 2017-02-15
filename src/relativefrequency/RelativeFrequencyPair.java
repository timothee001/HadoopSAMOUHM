package relativefrequency;

import java.io.IOException;

import invertedindex.ReadCSV;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
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

public class RelativeFrequencyPair extends Configured implements Tool {
	
	
	
   public static void main(String[] args) throws Exception {
     // System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new RelativeFrequencyPair(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf());
      job.setJarByClass(RelativeFrequencyPair.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setCombinerClass(Combine.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path("input")); 
      Path outputPath = new Path("outputRelativeFrequencyPair");
      FileOutputFormat.setOutputPath(job, outputPath);
      FileSystem hdfs = FileSystem.get(getConf());
	  if (hdfs.exists(outputPath))
	      hdfs.delete(outputPath, true);

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String[] words = value.toString().split(" ");

           for (String word : words) {
        	   word = word.toLowerCase();
               if (word.matches("^\\w+$") && !word.matches("-?\\d+(\\.\\d+)?")) { // check if it's a word and not a number
                   int count = 0;
                   for (String term : words) {
                	   term = term.toLowerCase();
                       if (term.matches("^\\w+$") && !term.equals(word)) {
                           context.write(new Text(word + "," + term), new Text("1"));
                           //System.out.println("emiting in mapper : "+ word + "," + term + " value : 1");
                           count++;
                       }
                   }
                   //System.out.println(word);
                   context.write(new Text(word + ",totalcount"), new Text(String.valueOf(count)));
                   //System.out.println("emiting in mapper :" + word + ",totalcount" + " value : "+ String.valueOf(count));
               }
           }
       }
   }

   private static class Combine extends Reducer<Text, Text, Text, Text> {
       public void reduce(Text key, Iterable<Text> values, Context context)
               throws IOException, InterruptedException {
           int count = 0;
           for (Text value : values) {
               count += Integer.parseInt(value.toString());
           }
           //System.out.println("emiting Combiner key : " + key + " value : "+ String.valueOf(count));
         
           context.write(key, new Text(String.valueOf(count)));
       }
   }

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
       TreeSet<Pair> topWordsPair = new TreeSet<>();
       double total = 0;

       public void reduce(Text key, Iterable<Text> values, Context context)
               throws IOException, InterruptedException {
           String keyStr = key.toString();
           int count = 0;

           for (Text value : values) {
               count += Integer.parseInt(value.toString());
           }

           if (keyStr.endsWith(",totalcount")) {
               total = count;
           } else {
               String[] pair = keyStr.split(",");
               topWordsPair.add(new Pair(count / total, pair[0], pair[1]));

               if (topWordsPair.size() > 100) {
                   topWordsPair.pollFirst();
               }
           }
       }

       protected void cleanup(Context ctxt) throws IOException,InterruptedException {
           while (!topWordsPair.isEmpty()) {
               Pair pair = topWordsPair.pollLast();
               ctxt.write(new Text(pair.getWord()), new Text(pair.getNeighbor()));
           }
       }

       
}
}
