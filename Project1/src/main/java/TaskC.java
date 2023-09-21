import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TaskC {

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {

        private Text country = new Text();
        private IntWritable ones = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split(",");

            country.set(split[2]);
            context.write(country, ones);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable citizens = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable citizen : values) {
                sum += citizen.get();
            }

            citizens.set(sum);

            context.write(key, citizens);
        }
    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Job job3 = Job.getInstance(conf, "Task C");

        job3.setJarByClass(TaskC.class);
        job3.setMapperClass(Map.class);
        job3.setReducerClass(Reduce.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}