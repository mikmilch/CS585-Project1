import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/*
Identify "outdated" FaceInPages. Return IDs and Names of persons that have not
accessed FaceIn for 90 days (i.e., no entries in the AccessLog in the last 90 days).
 */

public class TaskG {

    public static class AccessTimeMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private final IntWritable outKey = new IntWritable();
        private final IntWritable outValue = new IntWritable();


//    Mapper that reads in the csv file that maps the accessTime of each user
//    Consumes <id, AccessLogs>
//    Produces <user, access time> for users
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {

            // Access log
            String line = value.toString();

            // Split
            String[] split = line.split(",");

            outKey.set(Integer.parseInt(split[1]));  // Key = User that accessed a page
            outValue.set(Integer.parseInt(split[4])); // Value = the AccessTime

            context.write(outKey, outValue); // Write <user, access time>
        }
    }


//    Reducer that takes in the outputs from the mapper and sums that total
//    Consumes <user, [1 1 ... 1]>
//    Produces <user, count of relationships of the user>
    public static class AccessTimeReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        // one minute is the scale of access logs so 90 days = 129,600 days
        private static final int ninetyDays = 129600;

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int minValue = Integer.MAX_VALUE;
            for (IntWritable value : values) {
                minValue = Math.min(minValue, value.get());
            }
            if (minValue > ninetyDays){
                context.write(key, new IntWritable(minValue));
            }
        }
    }

    public static class PartG_Join_Mapper
            extends Mapper<LongWritable,Text,IntWritable,Text> {

        private final Map<String, String> accessLogMap = new HashMap<>();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);

            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);

            BufferedReader br = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
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
                context.write(new IntWritable(Integer.parseInt(values[0])), new Text(values[1]));
            }
        }
    }

    private static void simple(String input, String input1, String tempOutput, String output) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {


        long start = System.currentTimeMillis();
        // job1 driver code here
        Configuration conf1 = new Configuration();
        Job job1= Job.getInstance(conf1, "Get inactive IDs");

        job1.setJarByClass(TaskG.class);
        job1.setMapperClass(AccessTimeMapper.class);
        //        job.setCombinerClass(PartB_Reducer.class);
        job1.setReducerClass(AccessTimeReducer.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(tempOutput));
        job1.waitForCompletion(true);


        //job 3 driver code here
        Configuration conf2 = new Configuration();
        Job job2= Job.getInstance(conf2, "Joining for name");

        job2.setJarByClass(TaskG.class);
        job2.setMapperClass(PartG_Join_Mapper.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        //accessing the output file from job
        job2.addCacheFile(new URI(tempOutput + "/part-r-00000"));

        job2.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job2, new Path(input1));
        FileOutputFormat.setOutputPath(job2, new Path(output));
        job2.waitForCompletion(true);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Simple Time Taken: " + timeTaken);
    }


    public static void main(String[] args) throws Exception {

        String inputFaceInPageTest = "hdfs://localhost:9000/Project1/Testing/faceInPageTest.csv";
        String inputFaceInPage = "hdfs://localhost:9000/Project1/Final/faceInPage.csv";
        String inputAccessLogsTest = "hdfs://localhost:9000/Project1/Testing/accessLogsTest.csv";
        String inputAccessLogs = "hdfs://localhost:9000/Project1/Final/accessLogs.csv";

        String hdfsSimpleOutputTest = "hdfs://localhost:9000/Project1/Output/TaskG/Test/Simple";
        String hdfsSimpleOutput = "hdfs://localhost:9000/Project1/Output/TaskG/Final/Simple";


//        "hdfs://localhost:9000/Project1/Output/TaskG/Temp/Final/Advanced";
        String hdfsTempSimpleOutputTest = "file:///C:/Users/nickl/OneDrive/Desktop/output/Temp/TaskG/Test/Simple/";
//        "hdfs://localhost:9000/Project1/Output/TaskG/Test/Temp/Simple";
        String hdfsTempSimpleOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/Temp/TaskG/Final/Simple/";
//        "hdfs://localhost:9000/Project1/Output/TaskG/Temp/Final/Simple";



        System.out.println("Task G\n");
        System.out.println("Now Running Simple Methods");

        System.out.println("Running Test Files");
        simple(inputAccessLogsTest, inputFaceInPageTest, hdfsTempSimpleOutputTest, hdfsSimpleOutputTest);
        System.out.println("Running Actual Files");
        simple(inputAccessLogs, inputFaceInPage, hdfsTempSimpleOutput, hdfsSimpleOutput);
    }
}