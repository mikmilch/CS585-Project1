import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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

public class TaskB {
    public static class PartB_Mapper
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
    public static class PartB_Reducer
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
            extends Mapper<IntWritable,IntWritable,IntWritable,IntWritable> {

    }

    public static void main(String[] args) throws Exception {

        long timeNow = System.currentTimeMillis();

        // job1 driver code here
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf, "Top 8 most accessed pages");

        job.setJarByClass(TaskB.class);
        job.setMapperClass(PartB_Mapper.class);
//        job.setCombinerClass(PartB_Reducer.class);
        job.setReducerClass(PartB_Reducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/mikaelamilch/Downloads/data-2/Testing/accessLogsTest.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/testb_output"));
        job.waitForCompletion(true);

        // job2 driver code here!!

        // set up a timer to count running time
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
