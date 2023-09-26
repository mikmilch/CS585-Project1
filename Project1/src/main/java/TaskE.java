import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;


public class TaskE {

//    Mapper that reads in the csv file and counts the total amount of pages each user has accessed
//    Consumes <id, accessLogs>
//    Produces <user, 1>
    public static class TotalMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text user = new Text();
        private IntWritable ones = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Access Log
            String line = value.toString();

            // Split by Column
            String[] split = line.split(",");

//            System.out.println(split[0]);
            user.set(split[1]); // Key = User
            context.write(user, ones); // Write <key, value> = <User, 1>
        }
    }

//    Reducer that takes in the outputs from the mapper and counts all Total accesses of each user
//    Consumes <User, [1 1 ... 1]>
//    Produces <User, count of all accesses>
    public static class TotalReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable accesses = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0; // Count

            // For each access
            for (IntWritable access : values) {
                sum += access.get(); // Add to the count
            }

            accesses.set(sum); // Value = Count of accesses by a user

            context.write(key, accesses); // Write <key, value> = <User, Count>
        }
    }


//    Mapper that reads in the csv file and counts the total amount of distinct pages each user has accessed
//    Consumes <id, accessLogs>
//    Produces <user, 1> only if distinct
    public static class DistinctMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text user = new Text();
        private IntWritable ones = new IntWritable(1);

        ArrayList<Text> distinctList = new ArrayList<Text>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // AccessLog
            String line = value.toString();

            // Split by Column
            String[] split = line.split(",");

            // If distinct
            if (!distinctList.contains(new Text(split[1] + " " + split[2]))) {
                user.set(split[1]); // Key = User
                distinctList.add(new Text(split[1] + " " + split[2])); // Add to the list
                context.write(user, ones); // Write <key, value> = <User, 1>
            }
        }
        }


//    Reducer that takes in the outputs from the mapper and counts all distinct accesses of each user
//    Consumes <User, [1 1 ... 1]>
//    Produces <User, count of all distinct accesses>
    public static class DistinctReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable accesses = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0; // Count

            // For each access
            for (IntWritable access : values) {
                sum += access.get(); // add to the count
            }

            accesses.set(sum); // Value = Count of distinct access

            context.write(key, accesses); // Wrote <key, values> = <User, Count of distinct access>
        }
    }


//    Mapper that reads output from the output of the previous Map-Reduce Job and counts the total amount of  pages each user has accessed
//    Consumes <id, <User, count of all accesses>>
//    Produces <User, count of all accesses>
    public static class TotalJoinMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split("\t");

            outkey.set(split[0]); // Key = User
            outvalue.set("T" + split[1]); // Value = "T" + Count of all accesses
            context.write(outkey, outvalue); // Write <key, value> = <User, "T" + count of all accesses>
        }
    }


//    Mapper that reads output from the output of the previous Map-Reduce Job and counts the total amount of distinct pages each user has accessed
//    Consumes <id, <User, count of all distinct accesses>>
//    Produces <User, count of all distinct accesses>
    public static class DistinctJoinMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split("\t");

            outkey.set(split[0]); // Key = User
            outvalue.set("D" + split[1]); // Value = "D" + count of all distinct accesses
            context.write(outkey, outvalue); // Write <key, value> = <User, count of all distinct accesses>
        }
    }

//    Mapper that reads in the csv file and count the country
//    Consumes <id, FaceInPage>
//    Produces <id, "F" + Name>
    public static class FaceInMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split(",");

            outkey.set(split[0]); // Key = User
            outvalue.set("F" + split[1]); // Value = "F" + Name

            context.write(outkey, outvalue); // Write <key, value> = <User, "F" + Name>


        }
    }

//    Reducer that takes in the outputs from the mappers and joins
//    Consumes <id, Name/All Accesses/Distinct Access>
//    Produces <Name, (All Accesses, Distinct Accesses)>
    public static class JoinReduce extends Reducer<Text, Text, Text, Text> {

        private ArrayList<Text> totalList = new ArrayList<Text>();
        private ArrayList<Text> distinctList = new ArrayList<Text>();

        private ArrayList<Text> faceInList = new ArrayList<Text>();

        private Text outvalue = new Text();

        private String joinType = null;

        // Setup phase
        // Get the Join Type
        public void setup(Context context){
            joinType = context.getConfiguration().get("join.type");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Clears the lists
            totalList.clear();
            distinctList.clear();
            faceInList.clear();

            // For each output of the mappers
            for(Text value : values){

                // If from FaceInMap
                if (value.charAt(0) == 'F'){
                    faceInList.add(new Text(value.toString().substring(1)));
                }
                // If from Total Accesses
                else if (value.charAt(0) == 'T'){

                    totalList.add(new Text(value.toString().substring(1)));
                }
                else{
                    distinctList.add(new Text(value.toString().substring(1)));
                }
            }
            executeJoinLogic(context);
        }

        private void executeJoinLogic(Context context) throws IOException, InterruptedException {

            // For each user
            for (Text F : faceInList){

                // If No Accesses
                if (totalList.size() == 0){
                    context.write(F, new Text("Total: 0 , Distinct: 0"));
                }
                else {
                    for (Text T : totalList) {

                        for (Text D : distinctList) {
                            outvalue.set("Total: " + T + " , Distinct: " + D);
                            context.write(F, outvalue); // Write <name, (Total Accesses, Distinct Accesses)>
                        }
                    }
                }
            }
        }
    }

    private static void simple(String input, String input1, String tempOutput, String tempOutput1, String output) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Get Total Access By Each User");

        job.setJarByClass(TaskE.class);
        job.setMapperClass(TotalMap.class);
        job.setReducerClass(TotalReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(tempOutput));
        job.waitForCompletion(true);



        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Get Distinct Pages Accessed");

        job1.setJarByClass(TaskE.class);
        job1.setMapperClass(DistinctMap.class);
//        job1.setCombinerClass(DistinctReduce.class);
        job1.setReducerClass(DistinctReduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(tempOutput1));
        job1.waitForCompletion(true);



        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Join");
        

        job2.setJarByClass(TaskE.class);
        MultipleInputs.addInputPath(job2,new Path(tempOutput),TextInputFormat.class,TotalJoinMap.class);
        MultipleInputs.addInputPath(job2,new Path(tempOutput1),TextInputFormat.class,DistinctJoinMap.class);
        MultipleInputs.addInputPath(job2, new Path(input1), TextInputFormat.class,FaceInMap.class);

        job2.setReducerClass(JoinReduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(output));
        job2.waitForCompletion(true);
        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Simple Approach Time Taken: " + timeTaken);
    }

    private static void advanced(String input, String input1, String tempOutput, String tempOutput1, String output) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Get Total Access By Each User");

        job.setJarByClass(TaskE.class);
        job.setMapperClass(TotalMap.class);
        job.setCombinerClass(TotalReduce.class);
        job.setReducerClass(TotalReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(tempOutput));
        job.waitForCompletion(true);



        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Get Distinct Pages Accessed");

        job1.setJarByClass(TaskE.class);
        job1.setMapperClass(DistinctMap.class);
        job1.setCombinerClass(DistinctReduce.class);
        job1.setReducerClass(DistinctReduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(tempOutput1));
        job1.waitForCompletion(true);



        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Join");
        

        job2.setJarByClass(TaskE.class);
        MultipleInputs.addInputPath(job2,new Path(tempOutput),TextInputFormat.class,TotalJoinMap.class);
        MultipleInputs.addInputPath(job2,new Path(tempOutput1),TextInputFormat.class,DistinctJoinMap.class);
        MultipleInputs.addInputPath(job2, new Path(input1), TextInputFormat.class,FaceInMap.class);

        job2.setReducerClass(JoinReduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(output));
        job2.waitForCompletion(true);
        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Advanced Approach Time Taken: " + timeTaken);
    }

    public static void main(String[] args) throws Exception {

        String inputFaceInPageTest = "hdfs://localhost:9000/Project1/Testing/faceInPageTest.csv";
        String inputFaceInPage = "hdfs://localhost:9000/Project1/Final/faceInPage.csv";
        String inputAccessLogsTest = "hdfs://localhost:9000/Project1/Testing/accessLogsTest.csv";
        String inputAccessLogs = "hdfs://localhost:9000/Project1/Final/accessLogs.csv";

        String hdfsAdvancedOutputTest = "hdfs://localhost:9000/Project1/Output/TaskE/Test/Advanced";
        String hdfsAdvancedOutput = "hdfs://localhost:9000/Project1/Output/TaskE/Final/Advanced";
        String hdfsSimpleOutputTest = "hdfs://localhost:9000/Project1/Output/TaskE/Test/Simple";
        String hdfsSimpleOutput = "hdfs://localhost:9000/Project1/Output/TaskE/Final/Simple";

        String hdfsTempAdvancedOutputTest = "hdfs://localhost:9000/Project1/Output/TaskE/Temp/Test/Advanced/0";
        String hdfsTempAdvancedOutput = "hdfs://localhost:9000/Project1/Output/TaskE/Temp/Final/Advanced/0";
        String hdfsTempSimpleOutputTest = "hdfs://localhost:9000/Project1/Output/TaskE/Test/Temp/Simple/0";
        String hdfsTempSimpleOutput = "hdfs://localhost:9000/Project1/Output/TaskE/Temp/Final/Simple/0";
        String hdfsTempAdvancedOutputTest1 = "hdfs://localhost:9000/Project1/Output/TaskE/Temp/Test/Advanced/1";
        String hdfsTempAdvancedOutput1 = "hdfs://localhost:9000/Project1/Output/TaskE/Temp/Final/Advanced/1";
        String hdfsTempSimpleOutputTest1 = "hdfs://localhost:9000/Project1/Output/TaskE/Test/Temp/Simple/1";
        String hdfsTempSimpleOutput1 = "hdfs://localhost:9000/Project1/Output/TaskE/Temp/Final/Simple/1";


        System.out.println("Now Running Simple Methods\n");

        System.out.println("Running Test Files");
        simple(inputAccessLogsTest, inputFaceInPageTest, hdfsTempSimpleOutputTest, hdfsTempSimpleOutputTest1, hdfsSimpleOutputTest);
        System.out.println("\nRunning Actual Files");
//        simple(inputAccessLogs, inputFaceInPage, hdfsTempSimpleOutput, hdfsTempSimpleOutput1, hdfsSimpleOutput);

        System.out.println("\nNow Running Advanced Methods\n");

        System.out.println("Running Test Files");
        advanced(inputAccessLogsTest, inputFaceInPageTest, hdfsTempAdvancedOutputTest, hdfsTempAdvancedOutputTest1, hdfsAdvancedOutputTest);
        System.out.println("\nRunning Actual Files");
//        advanced(inputAccessLogs, inputFaceInPage, hdfsTempAdvancedOutput, hdfsTempAdvancedOutput1, hdfsAdvancedOutput);

    }
    
    
    
}
