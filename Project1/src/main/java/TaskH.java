import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

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


    public static class AverageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {

        private int count = 0;
        private int sum = 0;

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {


        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Get Total Access By Each User");

        job.setJarByClass(TaskH.class);
        job.setMapperClass(AssociatesMapper.class);
        job.setReducerClass(AssociatesReduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        String input = "hdfs://localhost:9000/Project1/Testing/associatesTest.csv";
//        "file:///C:/Users/nickl/OneDrive/Desktop/data/Testing/accessTesting.csv";
        String output = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/TaskH/Count";

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, " ");

        String input1 = "hdfs://localhost:9000/Project1/Testing/faceInPageTest.csv";
        String output2 = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/taskH/Average";


        job2.setJarByClass(TaskH.class);
        job2.setMapperClass(AverageMap.class);
//        job2.setReducerClass(TaskE.JoinReduce.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(output));
        FileOutputFormat.setOutputPath(job2, new Path(output2));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
    
}
