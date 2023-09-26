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
import java.util.ArrayList;


public class TaskE {

//    Mapper to go thorugh access logs and count the total amount of pages each user has accessed
    public static class TotalMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text user = new Text();
        private IntWritable ones = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split(",");

            if (!split[1].equals("ByWho")){
                user.set(split[1]);
                context.write(user, ones);
//                context.write(user2, ones);
            }

        }
    }

//    Count all Total accesses mapped
    public static class TotalReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable accesses = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable access : values) {
                sum += access.get();
            }

            accesses.set(sum);

            context.write(key, accesses);
        }
    }



//    Count only distinct accesses
    public static class DistinctMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text user = new Text();
        private IntWritable ones = new IntWritable(1);

        ArrayList<Text> distinctList = new ArrayList<Text>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split(",");

            if (!split[1].equals("ByWho")){
                System.out.println(split[1] + " " + split[2]);
                System.out.println(distinctList.contains(new Text(split[1] + " " + split[2])));
                if (!distinctList.contains(new Text(split[1] + " " + split[2]))) {
                    user.set(split[1]);
                    distinctList.add(new Text(split[1] + " " + split[2]));
                    context.write(user, ones);
                }
                else{
                    System.out.println(split[1] + " " + split[2]);
                }
            }
//            System.out.println(distinctList);

        }
    }

    public static class DistinctReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable accesses = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable access : values) {
                sum += access.get();
            }

            accesses.set(sum);

            context.write(key, accesses);
        }
    }


    public static class TotalJoinMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split("\t");

            outkey.set(split[0]);
            outvalue.set("T" + split[1]);
            context.write(outkey, outvalue);
        }
    }


    public static class DistinctJoinMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split("\t");

            outkey.set(split[0]);
            outvalue.set("D" + split[1]);
            context.write(outkey, outvalue);
        }
    }

    public static class FaceInMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

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

        private ArrayList<Text> totalList = new ArrayList<Text>();
        private ArrayList<Text> distinctList = new ArrayList<Text>();

        private ArrayList<Text> faceInList = new ArrayList<Text>();

        private Text outvalue = new Text();

        private String joinType = null;
        public void setup(Context context){
            joinType = context.getConfiguration().get("join.type");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            totalList.clear();
            distinctList.clear();
            faceInList.clear();

            for(Text test : values){

                if (test.charAt(0) == 'F'){
                    faceInList.add(new Text(test.toString().substring(1)));
                }
                else if (test.charAt(0) == 'T'){

                    totalList.add(new Text(test.toString().substring(1)));
                }
                else{
                    distinctList.add(new Text(test.toString().substring(1)));
                }
            }
            executeJoinLogic(context);
        }

        private void executeJoinLogic(Context context) throws IOException, InterruptedException {
            for (Text F : faceInList){

                if (totalList.size() == 0){
                    context.write(F, new Text("Total: 0 , Distinct: 0"));
                }
                else {
                    for (Text T : totalList) {

                        for (Text D : distinctList) {
//                        System.out.println("F: " + F + ", T: " + T + ", D: " + D);
                            outvalue.set("Total: " + T + " , Distinct: " + D);
                            context.write(F, outvalue);
                        }
                    }
                }

            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Get Total Access By Each User");

        job.setJarByClass(TaskE.class);
        job.setMapperClass(TotalMap.class);
        job.setReducerClass(TotalReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String input = "hdfs://localhost:9000/Project1/Testing/accessLogsTest.csv";
//        "file:///C:/Users/nickl/OneDrive/Desktop/data/Testing/accessTesting.csv";
        String output = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/taskE/total";

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);



        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Get Distinct Pages Accessed");

        job1.setJarByClass(TaskE.class);
        job1.setMapperClass(DistinctMap.class);
        job1.setReducerClass(DistinctReduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        String output1 = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/taskE/distinct";

        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output1));
        job1.waitForCompletion(true);

        
        
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Join");

        String input1 = "hdfs://localhost:9000/Project1/Testing/faceInPageTest.csv";
        String output2 = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output/taskE/Final";


        job2.setJarByClass(TaskE.class);
        MultipleInputs.addInputPath(job2,new Path(output),TextInputFormat.class,TotalJoinMap.class);
        MultipleInputs.addInputPath(job2,new Path(output1),TextInputFormat.class,DistinctJoinMap.class);
        MultipleInputs.addInputPath(job2, new Path(input1), TextInputFormat.class,FaceInMap.class);

        job2.setReducerClass(JoinReduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(output2));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
    
    
    
}
