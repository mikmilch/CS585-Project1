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

/*
Identify "outdated" FaceInPages. Return IDs and Names of persons that have not
accessed FaceIn for 90 days (i.e., no entries in the AccessLog in the last 90 days).
 */

public class TaskG {

    public static class PartG_Access_Time_Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private final IntWritable outKey = new IntWritable();
        private final IntWritable outValue = new IntWritable();


        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {

            String[] valueString = value.toString().split(",");

            outKey.set(Integer.parseInt(valueString[1]));   // 1 is the ByWho column
            outValue.set(Integer.parseInt(valueString[4])); // 4 is the AccessTime column

            context.write(outKey, outValue);
        }
    }

    public static class PartG_Name_Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final IntWritable outKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            String[] valueString = value.toString().split(",");

            outKey.set(Integer.parseInt(valueString[0]));   // 0 is the ID column
            outValue.set(new Text(valueString[1])); // 1 is the Name column

            context.write(outKey, outValue);
        }
    }

    public static class PartG_Access_Time_Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        // one minute is the scale of access logs so 90 days = 129,600 days
        private static final int ninetyDays = 129600;

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int minValue = Integer.MAX_VALUE;
            for (IntWritable value : values) {
                minValue = Math.min(minValue, value.get());
            }
            if (minValue > ninetyDays){
                context.write(key, new IntWritable(minValue));
            }
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        private static final int ninetyDays = 129600;

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String accessTime = null;
            String name = null;

            for (Text value : values) {
                String stringValue = value.toString();
                if (stringValue.matches("-?\\d+(\\.\\d+)?")) {
                    accessTime = stringValue;
                } else {
                    name = stringValue;
                }
            }

            if (accessTime != null && name != null) {
                context.write(key, new Text(name));
            }
        }
    }


    public static void main(String[] args) throws Exception {

    // job1 driver code here
        Configuration conf1 = new Configuration();
        Job job1= Job.getInstance(conf1, "Get inactive IDs");

        job1.setJarByClass(TaskG.class);
        job1.setMapperClass(PartG_Access_Time_Mapper.class);
//        job.setCombinerClass(PartB_Reducer.class);
        job1.setReducerClass(PartG_Access_Time_Reducer.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job1, new Path("/Users/mikaelamilch/Downloads/data-2/Testing/accessLogsTest.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/testg_output/job1"));
        job1.waitForCompletion(true);

    //job 2 driver code here
        Configuration conf2 = new Configuration();
        Job job2= Job.getInstance(conf2, "Get Name");

        job2.setJarByClass(TaskG.class);
        job2.setMapperClass(PartG_Name_Mapper.class);
        //job.setCombinerClass(PartB_Reducer.class);
        //job2.setReducerClass(PartG_Access_Time_Reducer.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path("/Users/mikaelamilch/Downloads/data-2/Testing/faceInPageTest.csv"));
        FileOutputFormat.setOutputPath(job2, new Path("/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/testg_output/job2"));
        job2.waitForCompletion(true);

    //job 3 driver code here
        Configuration conf3 = new Configuration();
        Job job3= Job.getInstance(conf3, "Get Name");

        job3.setJarByClass(TaskG.class);
        //job3.setMapperClass(PartG_Name_Mapper.class);
        //job3.setCombinerClass(JoinReducer.class);
        job3.setReducerClass(JoinReducer.class);

        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);

        String output1 = "/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/testg_output/job1";
        String output2 = "/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/testg_output/job2";

        MultipleInputs.addInputPath(job3, new Path(output2), TextInputFormat.class, PartG_Name_Mapper.class);
        MultipleInputs.addInputPath(job3, new Path(output1), TextInputFormat.class, PartG_Access_Time_Mapper.class);

        FileOutputFormat.setOutputPath(job3, new Path("/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/testg_output/job3"));
        job3.waitForCompletion(true);
    }
}