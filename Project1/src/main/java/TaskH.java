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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.io.IOException;

public class TaskH {


//    Mapper that reads in the csv file and relationships between associates
//    Consumes <id, Associates>
//    Produces <user, 1> for both users of an existing relationship
    public static class AssociatesMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

        private IntWritable user1 = new IntWritable();
        private IntWritable user2 = new IntWritable();
        private IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Associate
            String line = value.toString();

            // Split
            String[] split = line.split(",");

            // Key = User
            user1 = new IntWritable(Integer.parseInt(split[1]));
            user2 = new IntWritable(Integer.parseInt(split[2]));

            // <key, value> = <User, 1>
            context.write(user1, one);
            context.write(user2, one);
        }
    }


//    Reducer that takes in the outputs from the mapper and sums that total
//    Consumes <user, [1 1 ... 1]>
//    Produces <user, count of relationships of the user>
    public static class AssociatesReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable count = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {

            int sum = 0; // Count

            // For each associate
            for (IntWritable value : values) {
                sum += value.get(); // Add to the sum
            }

            count.set(sum); // Value = count

            context.write(key, count); // Write <key, value> = <user, count of relationships of the user>
        }
    }


//    Mapper that reads in the csv file and gets the average relationships of users
//    Consumes <id, <user, count of relationships of the user>>
//    Produces <user, count of relationships of the user>
    public static class AverageMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private IntWritable outkey = new IntWritable();
        private IntWritable outvalue = new IntWritable();


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Key = user
            outkey.set(Integer.parseInt(value.toString().split("\t")[0]));

            // Value = count of relationships of the user
            outvalue.set(Integer.parseInt(value.toString().split("\t")[1]));

            // Write <key, value> = <user, count of relationships of the user>
            context.write(outkey, outvalue);

        }

    }


//    Reducer that takes in the outputs from the mapper and gets the average
//    Consumes <user, count of relationships of the user>
//    Produces <average, >
    public static class AverageReducer extends Reducer<IntWritable, IntWritable, FloatWritable, NullWritable> {

        private int count = 0;
        private int sum = 0;

        private FloatWritable avg = new FloatWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {

            // For each value
            for (IntWritable value : values) {
                sum += value.get(); // Add to sum
                count++; // Count
            }

        }

        @Override
        protected void cleanup(Reducer<IntWritable, IntWritable, FloatWritable, NullWritable>.Context context) throws IOException, InterruptedException {

            // Write <key, value> = <average, >
            avg.set(sum/count);
            context.write(avg, null);
        }

    }


//    Reducer that takes in outputs from the two Map-Reduce jobs and joins based number of relationships of each user
//    Consumes <id, Count of relationships>
//    Produces <id, > if more popular than average
    public static class PopularMap extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

        private IntWritable outkey = new IntWritable();
        private IntWritable outvalue = new IntWritable();

        private float avg;


        // Setup phase
        // Stores in memory the average
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);

            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);


            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

            String line = reader.readLine();

            avg = Float.parseFloat(line);

            IOUtils.closeStream(reader);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // For each user
            String line = value.toString();

            // Split
            String[] split = line.split("\t");

            // If more popular than average
            if (Integer.parseInt(split[1]) > avg){
                outkey.set(Integer.parseInt(split[0])); // Key = id
                context.write(outkey, null); // Write <id, >
            }
        }

    }


    private static void simple(String input, String tempOutput, String tempOutput1, String output) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Get Total Access By Each User");

        job.setJarByClass(TaskH.class);
        job.setMapperClass(AssociatesMapper.class);
        job.setReducerClass(AssociatesReduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(tempOutput));
        job.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, " ");

        job2.setJarByClass(TaskH.class);
        job2.setMapperClass(AverageMap.class);
        job2.setReducerClass(AverageReducer.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job2, new Path(tempOutput));
        FileOutputFormat.setOutputPath(job2, new Path(tempOutput1));
        job2.waitForCompletion(true);
//        System.exit(job2.waitForCompletion(true) ? 0 : 1);



        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, " ");

        job3.setJarByClass(TaskH.class);
        job3.setMapperClass(PopularMap.class);
        //job2.setReducerClass(TaskH.AverageReducer.class);

        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(NullWritable.class);

        job3.addCacheFile(new URI(tempOutput1 + "/part-r-00000")); //

        job3.setNumReduceTasks(0);


        FileInputFormat.addInputPath(job3, new Path(tempOutput));
        FileOutputFormat.setOutputPath(job3, new Path(output));
        job3.waitForCompletion(true);
        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Simple Time Taken: " + timeTaken);
    }


    public static void main(String[] args) throws Exception {

        String inputAssociatesTest = "hdfs://localhost:9000/Project1/Testing/associatesTest.csv";
        String inputAssociates = "hdfs://localhost:9000/Project1/Final/associates.csv";

        String hdfsAdvancedOutputTest = "hdfs://localhost:9000/Project1/Output/TaskH/Test/Advanced";
        String hdfsAdvancedOutput = "hdfs://localhost:9000/Project1/Output/TaskH/Final/Advanced";
        String hdfsSimpleOutputTest = "hdfs://localhost:9000/Project1/Output/TaskH/Test/Simple";
        String hdfsSimpleOutput = "hdfs://localhost:9000/Project1/Output/TaskH/Final/Simple";


        String hdfsTempSimpleOutputTest = "file:///C:/Users/nickl/OneDrive/Desktop/output/Temp/TaskH/Test/Simple/0";
//        "hdfs://localhost:9000/Project1/Output/TaskH/Test/Temp/Simple";
        String hdfsTempSimpleOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/Temp/TaskH/Final/Simple/0";
//        "hdfs://localhost:9000/Project1/Output/TaskH/Temp/Final/Simple";
        String hdfsTempSimpleOutputTest1 = "file:///C:/Users/nickl/OneDrive/Desktop/output/Temp/TaskH/Test/Simple/1";
//        "hdfs://localhost:9000/Project1/Output/TaskH/Test/Temp/Simple";
        String hdfsTempSimpleOutput1 = "file:///C:/Users/nickl/OneDrive/Desktop/output/Temp/TaskH/Final/Simple/1";
//        "hdfs://localhost:9000/Project1/Output/TaskH/Temp/Final/Simple";


        System.out.println("Task H\n");
        System.out.println("Now Running Simple Methods");

        System.out.println("Running Test Files");
        simple(inputAssociatesTest, hdfsTempSimpleOutputTest, hdfsTempSimpleOutputTest1, hdfsSimpleOutputTest);
        System.out.println("Running Actual Files");
        simple(inputAssociates, hdfsTempSimpleOutput, hdfsTempSimpleOutput1, hdfsSimpleOutput);

    }

}