import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TaskC {

//    Mapper that reads in the csv file and count the country
//    Consumes <id, FaceInPage>
//    Produces <country, 1> based on the country of that user
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text country = new Text();
        private final IntWritable ones = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // FaceInPage user
            String line = value.toString();

            // Split by Column
            String[] split = line.split(",");

            if (!split[2].equals("Nationality")) {
                country.set(split[2]); // Key = Country
                context.write(country, ones); // Write <key,value> = <Country, 1>
            }
        }
    }

//    Reducer that takes in the outputs from the mapper and sums the total
//    Consumes <country, [1 1 ... 1]>
//    Produces <country, count of citizens in that country>
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable citizens = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0; // Count

            // For each citizen in a country
            for (IntWritable citizen : values) {
                sum += citizen.get(); // Add to the sum
            }

            citizens.set(sum); // Value = Count of citizens in that country

            context.write(key, citizens); // Write <key, value> = <Country, Count of Citizens in that Country>
        }
    }

    public static void main(String[] args) throws Exception {

//        Report for each country, how many of its citizens have a FaceInPage

//        Job Driver

        long start = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Count ");

        job1.setJarByClass(TaskC.class);
        job1.setMapperClass(Map.class);
//        job1.setCombinerClass(Reduce.class);
        job1.setReducerClass(Reduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        String input = "hdfs://localhost:9000/Project1/Testing/faceInPageTest.csv";
        String output = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/taskC/Final";

        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);
//        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}