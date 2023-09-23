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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TaskD {

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

        private Text test1 = new Text();
        private Text test2 = new Text();

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



            for (Text F : faceInList){

                if (countList.size() == 0){
                    context.write(F, new Text("0"));
                }
                else {
                    for (Text C : countList) {
//                        System.out.println("F: " + F + ", C: " + C);
                        context.write(F, C);
                    }
                }

            }
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job4 = Job.getInstance(conf, "Task D");

        job4.setJarByClass(TaskD.class);
        job4.setMapperClass(TaskD.Map.class);
        job4.setReducerClass(TaskD.Reduce.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);

        String input = "hdfs://localhost:9000/Project1/Testing/associatesTest.csv";
//        String input = "file:///C:/Users/nickl/OneDrive/Desktop/data/Testing/tested.csv";
        String output = "file:///C:/Users/nickl/OneDrive/Desktop/output";

        FileInputFormat.addInputPath(job4, new Path(input));
        FileOutputFormat.setOutputPath(job4, new Path(output));
        job4.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job job = Job.getInstance(conf2, "Task D1");

        job.setJarByClass(TaskD.class);

        String input1 = "hdfs://localhost:9000/Project1/Testing/faceInPageTest.csv";
        String output1 = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output2";

        MultipleInputs.addInputPath(job,
                new Path(output),
                TextInputFormat.class,
                AssociatesMap.class);
        MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class,
                FaceInMap.class);


        job.setReducerClass(JoinReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(output1));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
