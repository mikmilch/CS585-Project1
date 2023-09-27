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

    public static class AssociatesMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

        private IntWritable user1 = new IntWritable();
        private IntWritable user2 = new IntWritable();
        private IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split(",");

            if (!split[0].equals("FriendRel")){
                user1 = new IntWritable(Integer.parseInt(split[1]));
                user2 = new IntWritable(Integer.parseInt(split[2]));
                context.write(user1, one);
                context.write(user2, one);
            }
        }
    }

    public static class AssociatesReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable count = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {


            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            count.set(sum);

            context.write(key, count);
        }
    }



    public static class AverageMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private IntWritable outkey = new IntWritable();
        private IntWritable outvalue = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            outkey.set(Integer.parseInt(value.toString().split("\t")[0]));
            outvalue.set(Integer.parseInt(value.toString().split("\t")[1]));
            context.write(outkey, outvalue);

        }

    }


    public static class AverageReducer extends Reducer<IntWritable, IntWritable, FloatWritable, NullWritable> {

        private int count = 0;
        private int sum = 0;

        private FloatWritable avg = new FloatWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {

            //int count = 0;
            //int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
                count++;
            }


        }

        @Override
        protected void cleanup(Reducer<IntWritable, IntWritable, FloatWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            // For the top 10 most accessed page
            avg.set(sum/count);
            context.write(avg, null);
        }

    }

    public static class PopularMap extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

        private IntWritable outkey = new IntWritable();
        private IntWritable outvalue = new IntWritable();

        private float avg;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);

            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);


            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

            String line = reader.readLine();

            avg = Float.parseFloat(line);
//            System.out.println(3);

            IOUtils.closeStream(reader);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split("\t");

            if (Integer.parseInt(split[1]) > avg){
                outkey.set(Integer.parseInt(split[0]));
                context.write(outkey, null);
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