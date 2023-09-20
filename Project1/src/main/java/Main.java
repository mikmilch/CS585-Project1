
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;

import java.io.IOException;


// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {

    public static class testMapper extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

//            System.out.println(key);
            String line = value.toString();

            String[] split = line.split(",");

            if (split[2].equals("American")) {
                outkey.set(split[1]);
                outvalue.set(split[4]);

                context.write(outkey, outvalue);
            }
        }
    }

    public static class TestReducer extends Reducer<Text,Text,Text,IntWritable> {
        private Text result = new Text();

        public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {

            System.out.println(values);
            String line = values.toString();


//            System.out.println("value" + values);

//            String[] split = line.split(",");
//
//            String nationality = split[2];
//            System.out.println(nationality);

//            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception, ClassNotFoundException {

//        Task A
//        Report all FaceInPage users (name, and hobby) whose Nationality is the same as your
//        own Nationality (pick one). Note that nationalities in the data file are a random
//        sequence of characters unless you work with meaningful strings like “American”. This
//        is up to you
//        Input file : FaceInPage

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Task a");

        job1.setJarByClass(Main.class);
        job1.setMapperClass(testMapper.class);
//        job1.setCombinerClass(TestReducer.class);
//        job1.setReducerClass(TestReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);





        Configuration conf2 = new Configuration();

        Configuration conf3 = new Configuration();

        Configuration conf4 = new Configuration();

        Configuration conf5 = new Configuration();

        Configuration conf6 = new Configuration();

        Configuration conf7 = new Configuration();

        Configuration conf8 = new Configuration();





    }
}