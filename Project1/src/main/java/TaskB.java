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
import java.util.*;

public class TaskB {

    public static class Map extends Mapper<Object, Text, Text, IntWritable>{

        private Text outkey = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(",");

            outkey.set(split[2]);
            context.write(outkey, one);
        }
    }


    public static class Reduce
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            PriorityQueue<Counter> pq = new PriorityQueue<>(new ReducerComparator());

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

            pq.add(new Counter(key, result));

            while (!pq.isEmpty()) {
                System.out.println(pq.peek().key+" "+pq.poll().sumValue);
            }

            //pop out first 10 objects
            context.write(key, result);

            /*
                write whole object in priority queue
                    make new object to have key,result to put into the priority queue

                have to do a join to get name & nationality
                    mapper only job (bc only 10 records)
             */
        }

        class Counter {
            public Text key;
            public IntWritable sumValue;

            public Counter (Text key, IntWritable sumValue){
                this.key = key;
                this.sumValue = sumValue;
            }
        }

        class ReducerComparator implements Comparator<Counter> {
            // Overriding compare()method of Comparator
            public int compare(Counter s1, Counter s2) {
                return s1.sumValue.compareTo(s2.sumValue);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // 1. create a job object
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task B");

        // 2. map the jar class
        job.setJarByClass(TaskB.class);

        // 3. map both the mapper and reducer class
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
//        job.setCombinerClass(TaskB.class);

        // 4. set up the output key value data type class

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. set up the final output key value data type class

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        // 6. Specify the input and output path
        FileInputFormat.setInputPaths(job, new Path("/Users/mikaelamilch/Downloads/data/Testing/accessLogsTest.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Project1/testb_output"));
        // maybe need to change args to args[1] and args[2]
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //        System.exit(job.waitForCompletion(true) ? 0 : 1);


        // 7. submit the job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}


