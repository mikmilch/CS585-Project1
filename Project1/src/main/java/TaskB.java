import java.io.BufferedReader;
import java.io.FileReader;
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
    public static class AccessLogMapper
            extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private final IntWritable outKey = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            String[] valueString = value.toString().split(",");
            outKey.set(Integer.parseInt(valueString[2])); // 2 is the Whatpage column
            context.write(outKey,one);
        }
    }

    //REDUCER
    public static class AccessLogReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

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
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            topTen.add(new PageRankByCount(key.get(), sum));

            if (topTen.size() > 10) topTen.poll();
        }

        @Override
        protected void cleanup(Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            for(PageRankByCount pageCount: topTen){
                outKey.set(pageCount.pageId);
                outValue.set(pageCount.accesses);
                context.write(outKey, outValue);
            }
        }
    }



    // You need another mapper class to handle the join (use replicated (map-side) join)
    public static class PartB_Join_Mapper
            extends Mapper<LongWritable,Text,IntWritable,Text> {

        private Map<String, String> accessLogMap = new HashMap<>();
        private Text text = new Text();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
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
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            if(accessLogMap.containsKey(values[0])){
                System.out.println("In if statement with ID "+values[0]);
                context.write(new IntWritable(Integer.parseInt(values[0])), new Text(values[1]+"\t"+values[2]));
            }
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException  {

        // job1 driver code here
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf, "Top 10 most accessed pages");

        job.setJarByClass(TaskB.class);
        job.setMapperClass(AccessLogMapper.class);
//        job.setCombinerClass(PartB_Reducer.class);
        job.setReducerClass(AccessLogReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/mikaelamilch/Downloads/data-2/Testing/accessLogsTest.csv"));
        //FileOutputFormat.setOutputPath(job, new Path("/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/testb_output/job"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/mikaelamilch/Desktop/output"));
        job.waitForCompletion(true);

        // job2 driver code here!!
        Configuration conf2 = new Configuration();
        conf2.set("dataPath", "/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/taskb_buffer");
        Job job2= Job.getInstance(conf2, "Joining for name");

        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(PartB_Join_Mapper.class);
//        job.setCombinerClass(PartB_Reducer.class);
//        job2.setReducerClass(PartB_Reducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        //job2.addCacheFile(new URI("/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/testb_output/job/part-r-00000"));
        //accessing the output file from job
        job2.addCacheFile(new URI("/Users/mikaelamilch/Desktop/output/part-r-00000"));

        job2.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job2, new Path("/Users/mikaelamilch/Downloads/data-2/Testing/faceInPageTest.csv"));
        FileOutputFormat.setOutputPath(job2, new Path("/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/testb_output/job2"));
        job2.waitForCompletion(true);
    }
}
