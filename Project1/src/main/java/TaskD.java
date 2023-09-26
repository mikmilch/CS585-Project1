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

public class TaskD {

//    Mapper that reads in the csv file that maps relationships between two users
//    Consumes <id, Associates>
//    Produces <user, 1> for both users of a relationship
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text user1 = new Text();
        private final Text user2 = new Text();
        private final IntWritable ones = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Associate
            String line = value.toString();

            // Split by Column
            String[] split = line.split(",");

            user1.set(split[1]); // Key = Associate User
            user2.set(split[2]); // Key = Associate User
            // Write <key,value> = <User, 1>
            context.write(user1, ones);
            context.write(user2, ones);

        }
    }

//    Reducer that takes in the outputs from the mapper and sums that total
//    Consumes <user, [1 1 ... 1]>
//    Produces <user, count of relationships of the user>
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable relationship = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0; // Count

            // For each relationship of a user
            for (IntWritable relationships : values) {
                sum += relationships.get(); // Add to the sum
            }

            relationship.set(sum); // Value = Count of relationships of a user

            context.write(key, relationship); // Write <key, value> = <User, Count of Relationships>
        }
    }


//    Mapper that reads in the output from the output of the first Map-Reduce Job
//    Consumes <id, <User, Count of Relationships>>
//    Produces <User, Count of Relationships>
    public static class AssociatesMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // User, Count of Relationships>
            String line = value.toString();

            // Split
            String[] split = line.split("\t");

            outkey.set(split[0]); // Key = User
            outvalue.set("A" + split[1]); // Value = Count with the etter "A" to know it is an output of this mapper
            context.write(outkey, outvalue); // Write <key, value> = <User, "A" + Count>
        }
    }

//    Mapper that reads in the csv file and count the country
//    Consumes <id, FaceInPage>
//    Produces <id, Name>
    public static class FaceInMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // FaceInPage user
            String line = value.toString();

            // Split by coliumn
            String[] split = line.split(",");

            outkey.set(split[0]); // Key = id
            outvalue.set("F" + split[1]); // Value = "F" + Name

            context.write(outkey, outvalue); // Write <key, value> = <id, "F" + name>


        }
    }

//    Reducer that takes in outputs from the two mappers and joins based on the id of the users
//    Consumes <id, count/name>
//    Produces <Name, Count of relationships>
    public static class JoinReduce extends Reducer<Text, Text, Text, Text> {

        private ArrayList<Text> countList = new ArrayList<Text>();
        private ArrayList<Text> faceInList = new ArrayList<Text>();

        private String joinType = null;

        // Setup phase
        // Get the Join Type
        public void setup(Context context){
            joinType = context.getConfiguration().get("join.type");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Clears the lists
            countList.clear();
            faceInList.clear();

            // For each output of the mappers
            for(Text value : values){

                // If from FaceInMap
                if (value.charAt(0) == 'F'){
                    // Add to the faceInList
                    faceInList.add(new Text(value.toString().substring(1)));
                }
                else{
                    // Add to the countList
                    countList.add(new Text(value.toString().substring(1)));
                }
            }

            executeJoinLogic(context);
        }
        
        private void executeJoinLogic(Context context) throws IOException, InterruptedException {

            if (joinType.equals("inner")) {

                // For each user
                for (Text F : faceInList) {

                    // If No relationships as that user
                    if (countList.size() == 0) {
                        context.write(F, new Text("0"));
                    } else {
                        for (Text C : countList) {
                            context.write(F, C); // Write <name, count>
                        }
                    }
                }
            }
        }
    }

//    Mapper that takes in the outputs from the previous Map-Reduce Job and joins with FaceInPage
//    Consumes <id, FaceInPage>
//    Produces <name, count>
    public static class MapJoin extends Mapper<LongWritable, Text, Text, Text> {

        private HashMap<String, String> relationMap = new HashMap<>();
        private Text outvalue = new Text();

        // Setup by reading in the file to store in memory for join
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);


            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);

            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

            String line;

            while (StringUtils.isNotEmpty(line = reader.readLine())){
                try {
                    String[] split = line.split("\t");
                    relationMap.put(split[0], split[1]);
                }catch(Exception e){
                    System.out.println(e);
                }
            }
            IOUtils.closeStream(reader);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // FaceInPage user
            String line = value.toString();

            // Split by Column
            String[] split = line.split(",");

            // Relationship count stored in the hashmap
            String relationships = relationMap.get(split[0]);

            outvalue.set(new Text(relationships)); // Value = Count

            context.write(new Text(split[1]), outvalue); // <key, value> = <Name, Count>

        }
    }

    private static void simple() throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Count Relationships of Each FaceInPage User");

        job1.setJarByClass(TaskD.class);
        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        String input = "hdfs://localhost:9000/Project1/Testing/associatesTest.csv";
//        String input = "file:///C:/Users/nickl/OneDrive/Desktop/data/Testing/tested.csv";
        String output = "file:///C:/Users/nickl/OneDrive/Desktop/output/taskD/Simple";
//                "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/taskD/count";

        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Reduce Side Join to output name and count of relationships");

        job2.setJarByClass(TaskD.class);
//
        String input1 = "hdfs://localhost:9000/Project1/Testing/faceInPageTest.csv";
        String output1 = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/taskD/Final/Simple";
//
        MultipleInputs.addInputPath(job2,new Path(output),TextInputFormat.class,AssociatesMap.class);
        MultipleInputs.addInputPath(job2, new Path(input1), TextInputFormat.class, FaceInMap.class);

        job2.getConfiguration().set("join.type", "inner");


        job2.setReducerClass(JoinReduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(output1));
        job2.waitForCompletion(true);
        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Simple Approach Time Taken: " + timeTaken);
    }

    private static void advanced() throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Count Relationships of Each FaceInPage User");

        job1.setJarByClass(TaskD.class);
        job1.setMapperClass(TaskD.Map.class);
        job1.setReducerClass(TaskD.Reduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        String input = "hdfs://localhost:9000/Project1/Testing/associatesTest.csv";
//        String input = "file:///C:/Users/nickl/OneDrive/Desktop/data/Testing/tested.csv";
        String output = "file:///C:/Users/nickl/OneDrive/Desktop/output/taskD/Advanced";
//                "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/taskD/count";

        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);


        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Map Side Join to output name and count relationships");

        job3.setJarByClass(TaskD.class);
        job3.setMapperClass(MapJoin.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.addCacheFile(new URI(output + "/part-r-00000"));

        job3.setNumReduceTasks(0);

        String input1 = "hdfs://localhost:9000/Project1/Testing/faceInPageTest.csv";
        String output1 = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/taskD/Final/Advanced";

        FileInputFormat.addInputPath(job3, new Path(input1));
        FileOutputFormat.setOutputPath(job3, new Path(output1));
        job3.waitForCompletion(true);
        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Advanced Time Taken: " + timeTaken);

    }



    public static void main(String[] args) throws IOException, URISyntaxException,
    ClassNotFoundException, InterruptedException{


        simple();

        advanced();

    }
}
