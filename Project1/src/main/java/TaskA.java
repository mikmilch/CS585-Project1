
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;



public class TaskA {

//    Mapper that reads in the csv file and only select and write out the users with a specific nationality
//    Consumes <id, FaceInPage>
//    Produces <name, hobby> only if nationality matches
    public static class TaskAMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outkey = new Text();
        private final Text outvalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // FaceInPage user
            String line = value.toString();

            // Split by Column
            String[] split = line.split(",");

            // If nationality matches
            if (split[2].equals("American")) {
                outkey.set(split[1]); // Key = Name
                outvalue.set(split[4]); // Value = Hobby

                context.write(outkey, outvalue); // Write
            }
        }
    }


    public static void main(String[] args) throws Exception, ClassNotFoundException {

//        Task A
//        Report all FaceInPage users (name, and hobby) whose Nationality is the same as your
//        own Nationality (pick one). Note that nationalities in the data file are a random
//        sequence of characters unless you work with meaningful strings like “American”. This
//        is up to you
//        Input file : FaceInPage

//        Job Driver
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Select users with the same nationality (Chinese)");

        job1.setJarByClass(TaskA.class);
        job1.setMapperClass(TaskAMapper.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        String input = "hdfs://localhost:9000/Project1/Testing/faceInPageTest.csv";
        String output = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/taskA/Final";

        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
//        System.exit(job1.waitForCompletion(true) ? 0 : 1);
        job1.waitForCompletion(true);
        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);


    }
}