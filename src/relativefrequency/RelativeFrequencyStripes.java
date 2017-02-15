package relativefrequency;

import java.io.IOException;

import invertedindex.ReadCSV;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

public class RelativeFrequencyStripes extends Configured implements Tool {
	
	
	
   public static void main(String[] args) throws Exception {
     // System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new RelativeFrequencyStripes(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf());
      job.setJarByClass(RelativeFrequencyStripes.class);
     
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setCombinerClass(Combine.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path("input")); 
      Path outputPath = new Path("outputRelativeFrequencyStripe");
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
               if (word.matches("^\\w+$") && !word.matches("-?\\d+(\\.\\d+)?")) {
                  HashMap<String, Integer> stripe = new HashMap<>();

                   for (String term : words) {
                	   term = term.toLowerCase();
                       if (term.matches("^\\w+$") && !term.equals(word)) {
                           Integer count = stripe.get(term);                     
                           if(count==null){
                        	   stripe.put(term,1);
                           }else{
                        	   stripe.put(term,count+1);
                           }                                                 
                       }
                   }

                   StringBuilder stripeStr = new StringBuilder();
                   for (Entry entry : stripe.entrySet()) {
                       stripeStr.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
                   }

                   if (!stripe.isEmpty()) {
                       context.write(new Text(word), new Text(stripeStr.toString()));
                       //System.out.println("emiting in mapper :" + word + ",totalcount" + " value : "+ stripeStr.toString());
                   }
               }
           }
       }
   }

   private static class Combine extends Reducer<Text, Text, Text, Text> {
       public void reduce(Text key, Iterable<Text> values, Context context)
               throws IOException, InterruptedException {
    	   HashMap<String, Integer> stripe = new HashMap<>();

           for (Text value : values) {
               String[] stripes = value.toString().split(",");

               for (String termCountStr : stripes) {
                   String[] termCount = termCountStr.split(":");
                   String term = termCount[0];
                   int count = Integer.parseInt(termCount[1]);
                   Integer countSum = stripe.get(term);
                   if(countSum==null){
                	   stripe.put(term,count);
                   }else{
                	   stripe.put(term,count+countSum);
                   }                 
               }
           }

           StringBuilder stripeStr = new StringBuilder();
           for (Entry entry : stripe.entrySet()) {
               stripeStr.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
           }

           context.write(key, new Text(stripeStr.toString()));
           //System.out.println("emiting Combiner key : " + key + " value : "+ stripeStr.toString());
       }
   }

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
       TreeSet<Pair> topWordsPair = new TreeSet<>();

       public void reduce(Text key, Iterable<Text> values, Context context)
               throws IOException, InterruptedException {
           java.util.Map<String, Integer> stripe = new HashMap<>();
           double total = 0;
           String keyStr = key.toString();

           for (Text value : values) {
               String[] stripes = value.toString().split(",");

               for (String termCountStr : stripes) {
                   String[] termCount = termCountStr.split(":");
                   String term = termCount[0];
                   int count = Integer.parseInt(termCount[1]);

                   Integer countSum = stripe.get(term);
                   
                   if(countSum == null){
                	   stripe.put(term,count);
                   }else{
                	   stripe.put(term,countSum+count);
                   }             
                   total += count;
               }
           }

           for (Entry<String, Integer> entry : stripe.entrySet()) {
               topWordsPair.add(new Pair(entry.getValue() / total, keyStr, entry.getKey()));

               if (topWordsPair.size() > 100) {
                   topWordsPair.pollFirst();
               }
           }
       }

       protected void cleanup(Context ctxt) throws IOException, InterruptedException {
           while (!topWordsPair.isEmpty()) {
               Pair pair = topWordsPair.pollLast();
               ctxt.write(new Text(pair.getWord()), new Text(pair.getNeighbor()));
           }
       }

       
   }
}
