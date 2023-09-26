import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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

//    Maps relationships between two users
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {

        private Text user1 = new Text();
        private Text user2 = new Text();
        private IntWritable ones = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split(",");

            if (!split[1].equals("PersonA_ID")){
                user1.set(split[1]);
                user2.set(split[2]);
                context.write(user1, ones);
                context.write(user2, ones);
            }

        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable relationship = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable relationships : values) {
                sum += relationships.get();
            }

            relationship.set(sum);

            context.write(key, relationship);
        }
    }


    public static class AssociatesMap extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split("\t");

            outkey.set(split[0]);
            outvalue.set("A" + split[1]);
            context.write(outkey, outvalue);
        }
    }

    public static class FaceInMap extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split(",");

            if (!split[0].equals("ID")){
                outkey.set(split[0]);
                outvalue.set("F" + split[1]);

                context.write(outkey, outvalue);
            }


        }
    }

    public static class JoinReduce extends Reducer<Text, Text, Text, Text> {

        private ArrayList<Text> countList = new ArrayList<Text>();
        private ArrayList<Text> faceInList = new ArrayList<Text>();

        private String joinType = null;
        
        public void setup(Context context){
            joinType = context.getConfiguration().get("join.type");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            countList.clear();
            faceInList.clear();

            for(Text test : values){

                if (test.charAt(0) == 'F'){
                    faceInList.add(new Text(test.toString().substring(1)));
                }
                else{

                    countList.add(new Text(test.toString().substring(1)));
                }
            }

            executeJoinLogic(context);
        }
        
        private void executeJoinLogic(Context context) throws IOException, InterruptedException {

            if (joinType.equals("inner")) {
                for (Text F : faceInList) {

                    if (countList.size() == 0) {
                        context.write(F, new Text("0"));
                    } else {
                        for (Text C : countList) {
//                        System.out.println("F: " + F + ", C: " + C);
                            context.write(F, C);
                        }
                    }

                }
            }
        }
    }

    public static class MapJoin extends Mapper<Object, Text, Text, Text> {

        private HashMap<String, String> relationMap = new HashMap<>();

        private Text outvalue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
//            System.out.println("1");
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);


            FileSystem fs = FileSystem.get(context.getConfiguration());
//            System.out.println(path);
            FSDataInputStream fis = fs.open(path);
//            System.out.println(fis);

            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

            String line;

            while (StringUtils.isNotEmpty(line = reader.readLine())){
                try {
//                    System.out.println(line);
                    String[] split = line.split("\t");
                    relationMap.put(split[0], split[1]);
                }catch(Exception e){
                    System.out.println(e);
                }
            }
            IOUtils.closeStream(reader);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
//            System.out.println(line);

            String[] split = line.split(",");

            String relationships = relationMap.get(split[0]);
//            System.out.println("Rel: " + relationships);

            outvalue.set(new Text(relationships));

            context.write(new Text(split[1]), outvalue);

        }
    }

    private static void simple() throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Task D");

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
        Job job2 = Job.getInstance(conf2, "Task D1");

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
        Job job1 = Job.getInstance(conf, "Task D");

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
        Job job3 = Job.getInstance(conf3, "Map Side Join");

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

//        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }



    public static void main(String[] args) throws IOException, URISyntaxException,
    ClassNotFoundException, InterruptedException{


        simple();

        advanced();

    }
}
