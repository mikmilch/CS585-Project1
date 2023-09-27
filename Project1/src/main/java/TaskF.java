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
import java.util.HashSet;

public class TaskF {


//    Mapper that reads in the csv file and maps the relationship between FaceIn Users
//    Consumes <id, Associates>
//    Produces <user1, user2> and <user2, user1>
    public static class AssociatesMap extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Associate
            String line = value.toString();

            // Split by Column
            String[] split = line.split(",");

            // Write <key,value> = <User, User>
            context.write(new Text(split[1]), new Text("A" + split[2]));
            context.write(new Text(split[2]), new Text("A" + split[1]));
        }
    }


//    Mapper that reads in the csv file and maps the access between FaceIn Users
//    Consumes <id, AccessLog>
//    Produces <user1, user2>  where user1 is the person accessing user2's page
    public static class AccessMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Access Log
            String line = value.toString();

            // Split by Column
            String[] split = line.split(",");

            outkey.set(split[1]); // Key = User
            outvalue.set("L" + split[2]); // Value = "L" + User
            context.write(outkey, outvalue);// Write <key,value> = <User, User>
        }
    }


//    Reducer that takes in the outputs from the mappers and finds users that have a relationship with another but havent accessed
//    Consumes <user1, user2>
//    Produces <user, count of relationships of the user>
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private HashSet<String> AssociatesSet = new HashSet<>();
        private HashSet<String> AccessSet = new HashSet<>();


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            AssociatesSet.clear();
            AccessSet.clear();

            // For each output from mappers
            for (Text value : values) {

                // If Associates
                if (value.charAt(0) == 'A'){
                    // Add to Associates Set
                    AssociatesSet.add(key + " " + value.toString().substring(1));
                }
                else{
                    // Add to Access Set
                    AccessSet.add(key + " " + value.toString().substring(1));
                }
            }

            // If Associates Set contains the access then remove (Leaving only associates that have not accessed)
            for (String access : AccessSet){
                AssociatesSet.remove(access);
            }

            // For Remaining Associates
            for (String associate : AssociatesSet){
                // Write <key, value> = <User1, User2>
                context.write(new Text(associate.toString().split(" ")[0]), new Text(associate.toString().split(" ")[1]));
            }

        }
    }


    private static void simple(String associatesInput, String accessInput, String output) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();


        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Test");

        job1.setJarByClass(TaskF.class);


        MultipleInputs.addInputPath(job1,new Path(associatesInput), TextInputFormat.class, AssociatesMap.class);
        MultipleInputs.addInputPath(job1, new Path(accessInput), TextInputFormat.class, AccessMap.class);

        job1.setReducerClass(Reduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);


        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Simple Approach Time Taken: " + timeTaken);
    }

    public static void main(String[] args) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {

        String inputAssociatesTest = "hdfs://localhost:9000/Project1/Testing/associatesTest.csv";
        String inputAssociates = "hdfs://localhost:9000/Project1/Final/associates.csv";
        String inputAccessLogsTest = "hdfs://localhost:9000/Project1/Testing/accessLogsTest.csv";
        String inputAccessLogs = "hdfs://localhost:9000/Project1/Final/accessLogs.csv";

        String hdfsSimpleOutputTest = "hdfs://localhost:9000/Project1/Output/TaskF/Test/Simple";
        String hdfsSimpleOutput = "hdfs://localhost:9000/Project1/Output/TaskF/Final/Simple";


        System.out.println("Task F");
        System.out.println("Now Running Simple Methods\n");

        System.out.println("Running Test Files");
        simple(inputAssociatesTest, inputAccessLogsTest, hdfsSimpleOutputTest);
        System.out.println("Running Actual Files");
        simple(inputAssociates, inputAccessLogs, hdfsSimpleOutput);
    }
}

