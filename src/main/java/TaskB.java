import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class TaskB {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job task2 = Job.getInstance(conf, "job2");
    }
}
