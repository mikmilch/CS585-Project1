import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;
import java.net.URISyntaxException;

/*
Find the 10 most popular FaceIn pages, namely, those that got the most accesses based
on the AccessLog among all pages. Return Id, Name and Nationality.
 */
public class TaskB {

//    Mapper that reads in the csv file that maps each page accessed by users
//    Consumes <id, AccessLogs>
//    Produces <page, 1> for pages accessed
    public static class AccessLogMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private final IntWritable outKey = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {

            // Access Log
            String line = value.toString();

            // Split by Column
            String[] split = line.split(",");

            outKey.set(Integer.parseInt(split[2])); // Key = Page Accessed
            context.write(outKey,one); // Write <key, value> = <Page, 1>
        }
    }

//    Reducer that takes in the outputs from the mapper and sums the total
//    Consume <Page, [1 1 ... 1]>
//    Produces <Page, Count of accessed made to this page> but only the top ten through the use of a priority queue
    public static class AccessLogReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        public static class PageRankByCount implements Comparable<PageRankByCount> {

            public int pageId;
            public int accesses;

            public PageRankByCount(int pageId, int accesses) {
                this.pageId = pageId;
                this.accesses = accesses;
            }

            // override the compareTo() to make sure when comparison between two objects in PriorityQueue
            // they are comparing count of access

            @Override
            public int compareTo(PageRankByCount obj2) {
                return Integer.compare(this.accesses, obj2.accesses);
            }
        }

        private IntWritable outKey = new IntWritable();
        private IntWritable outValue = new IntWritable();

        // Stores a map of page ID and count
        private static PriorityQueue<PageRankByCount> topTen = new PriorityQueue<PageRankByCount>();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0; // Count

            for (IntWritable val : values) {
                sum += val.get(); // Add to the count
            }

            // Add to the pq
            topTen.add(new PageRankByCount(key.get(), sum));

            // If pq full, get rid of the lowest value
            if (topTen.size() > 10) topTen.poll();
        }

        @Override
        protected void cleanup(Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            // For the top 10 most accessed page
            for(PageRankByCount pageCount: topTen){
                outKey.set(pageCount.pageId); // Key = Page
                outValue.set(pageCount.accesses); // Value = Count of access
                context.write(outKey, outValue); // Write <key, value> = <Page, Count of accessed made to this page>
            }
        }
    }



//    Mapper that takes in the outputs from the previous Map-Reduce Job and joins with FaceInPage
//    Consumes <id, FaceInPage>
//    Produces <id, (name, nationality)>
    public static class JoinMapper extends Mapper<LongWritable,Text,IntWritable,Text> {

        private Map<String, String> accessLogMap = new HashMap<>();
        private Text text = new Text();

    // Setup by reading in the file to store in memory for join
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);

            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);

            BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            String line;

            while (StringUtils.isNotEmpty(line = br.readLine())) {
                try {
                    String[] split = line.split("\t");
                    accessLogMap.put(split[0], split[1]);
                }
                catch (Exception e){
                    System.out.println(e);
                }
            }
            // close the stream
            IOUtils.closeStream(br);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // FaceInPage user
            String line = value.toString();

            // Split
            String[] split = line.split(",");

            // If the current user is one of the top ten most accesedd
            if(accessLogMap.containsKey(split[0])){

                // Write <key, value> = <id, (name, nationality)>
                context.write(new IntWritable(Integer.parseInt(split[0])), new Text(split[1]+"\t"+split[2]));
            }
        }
    }

    private static void simple(String input, String input1, String tempOutput, String output) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();
        // job1 driver code here
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf, "Top 10 most accessed pages");

        job.setJarByClass(TaskB.class);
        job.setMapperClass(AccessLogMapper.class);
        job.setReducerClass(AccessLogReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(tempOutput));
        job.waitForCompletion(true);

        // job2 driver code here!!
        Configuration conf2 = new Configuration();
        Job job2= Job.getInstance(conf2, "Joining for name");

        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(JoinMapper.class);


        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.addCacheFile(new URI(tempOutput + "/part-r-00000"));

        job2.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job2, new Path(input1));
        FileOutputFormat.setOutputPath(job2, new Path(output));
        job2.waitForCompletion(true);
        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Simple Time Taken: " + timeTaken);
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException  {

        String inputFaceInPageTest = "hdfs://localhost:9000/Project1/Testing/faceInPageTest.csv";
        String inputFaceInPage = "hdfs://localhost:9000/Project1/Final/faceInPage.csv";
        String inputAccessLogsTest = "hdfs://localhost:9000/Project1/Testing/accessLogsTest.csv";
        String inputAccessLogs = "hdfs://localhost:9000/Project1/Final/accessLogs.csv";

        String hdfsSimpleOutputTest = "hdfs://localhost:9000/Project1/Output/TaskB/Test/Simple";
        String hdfsSimpleOutput = "hdfs://localhost:9000/Project1/Output/TaskB/Final/Simple";


//        "hdfs://localhost:9000/Project1/Output/TaskB/Temp/Final/Advanced";
        String hdfsTempSimpleOutputTest = "file:///C:/Users/nickl/OneDrive/Desktop/output/Temp/TaskB/Test/Simple/";
//        "hdfs://localhost:9000/Project1/Output/TaskB/Test/Temp/Simple";
        String hdfsTempSimpleOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/Temp/TaskB/Final/Simple/";
//        "hdfs://localhost:9000/Project1/Output/TaskB/Temp/Final/Simple";



        System.out.println("Task B\n");
        System.out.println("Now Running Simple Methods");

        System.out.println("Running Test Files");
        simple(inputAccessLogsTest, inputFaceInPageTest, hdfsTempSimpleOutputTest, hdfsSimpleOutputTest);
        System.out.println("Running Actual Files");
        simple(inputAccessLogs, inputFaceInPage, hdfsTempSimpleOutput, hdfsSimpleOutput);

    }
}
