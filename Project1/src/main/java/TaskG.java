import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

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

    public static class PartG_Join_Mapper
            extends Mapper<LongWritable,Text,IntWritable,Text> {

        private final Map<String, String> accessLogMap = new HashMap<>();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);

            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);

            BufferedReader br = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
            String line;

            while (StringUtils.isNotEmpty(line = br.readLine())) {
                try {
                    String[] split = line.split("\t");
                    accessLogMap.put(split[0], split[1]);
                }
                catch (Exception e){
                    System.out.println(e);
                }
            }
            // close the stream
            IOUtils.closeStream(br);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            if(accessLogMap.containsKey(values[0])){
                context.write(new IntWritable(Integer.parseInt(values[0])), new Text(values[1]));
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
        FileOutputFormat.setOutputPath(job1, new Path("/Users/mikaelamilch/Desktop/output"));
        job1.waitForCompletion(true);


    //job 3 driver code here
        Configuration conf2 = new Configuration();
        conf2.set("dataPath", "/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/taskg_buffer");
        Job job2= Job.getInstance(conf2, "Joining for name");

        job2.setJarByClass(TaskG.class);
        job2.setMapperClass(PartG_Join_Mapper.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        //accessing the output file from job
        job2.addCacheFile(new URI("/Users/mikaelamilch/Desktop/output/part-r-00000"));

        job2.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job2, new Path("/Users/mikaelamilch/Downloads/data-2/Testing/faceInPageTest.csv"));
        FileOutputFormat.setOutputPath(job2, new Path("/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/testg_output/job3"));
        job2.waitForCompletion(true);
    }
}