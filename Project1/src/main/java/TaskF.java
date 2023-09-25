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

    public static class AssociatesMap extends Mapper<LongWritable, Text, Text, Text> {


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split(",");

            if (!split[0].equals("FriendRel")){
//            System.out.println(split[0]);
                context.write(new Text(split[1]), new Text("A" + split[2]));
                context.write(new Text(split[2]), new Text("A" + split[1]));
            }


        }
    }

    public static class AccessMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split(",");

            if (!split[0].equals("AccessID")){
//                System.out.println(split[0]);
                outkey.set(split[1]);
                outvalue.set("L" + split[2]);
                context.write(outkey, outvalue);
            }


        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        HashSet<String> AssociatesSet = new HashSet<>();
        HashSet<String> AccessSet = new HashSet<>();

        int i = 0;
        int n = 0;
        int total = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            AssociatesSet.clear();
            AccessSet.clear();

            for (Text value : values) {
                if (value.charAt(0) == 'A'){
                    if (AssociatesSet.contains(key + " " + value.toString().substring(1))){
                        System.out.println("I: " + i++);
                        System.out.println(value);
                        System.out.println(key + " " + value.toString().substring(1));
                    }
                    AssociatesSet.add(key + " " + value.toString().substring(1));
//                    System.out.println("I: " + i++);
                }
                else{
                    AccessSet.add(key + " " + value.toString().substring(1));
                }
            }

//            for (String associate : AssociatesSet) {
//                for (String access : AccessSet) {
//                    if (!associate.equals(access)){
//                        context.write(new Text(access.toString().split(" ")[0]), new Text(access.toString().split(" ")[1]));
//                    }
//                    else{
//                        System.out.println(access);
//                    }
//                }
//            }

            for (String access : AccessSet){
                if (AssociatesSet.contains(access)){
//                    System.out.println(access);
                }
                AssociatesSet.remove(access);
//                System.out.println(access);
            }

            for (String associate : AssociatesSet){
                context.write(new Text(associate.toString().split(" ")[0]), new Text(associate.toString().split(" ")[1]));
            }

        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Test");

        job1.setJarByClass(TaskF.class);
//        job1.setMapperClass(AssociatesMap.class);


        String associatesInput = "hdfs://localhost:9000/Project1/Testing/associatesTest.csv";
        String accessInput = "hdfs://localhost:9000/Project1/Testing/accessLogsTest.csv";
//        String input = "file:///C:/Users/nickl/OneDrive/Desktop/data/Testing/tested.csv";
        String output = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/taskF";
//                "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/taskD/count";

        MultipleInputs.addInputPath(job1,new Path(associatesInput), TextInputFormat.class, AssociatesMap.class);
        MultipleInputs.addInputPath(job1, new Path(accessInput), TextInputFormat.class, AccessMap.class);
//
        job1.setReducerClass(Reduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);


//        FileInputFormat.addInputPath(job1, new Path(associatesInput));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);

    }
}

