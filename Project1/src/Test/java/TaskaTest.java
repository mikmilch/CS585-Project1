import org.junit.Test;

public class TaskaTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[2];

        /*
        1. put the data.txt into a folder in your pc
        2. add the path for the following two files.
            windows : update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            mac or linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

//        input[0] = "hdfs://localhost:9000/project0/data.txt";
//        input[1] = "hdfs://localhost:9000/project0/output.txt";

        input[0] = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/src/main/python/faceInPageTest.csv";
        input[1] = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project1/CS585-Project1/Project1/output";

        Main main = new Main();
        main.main(input);
    }

}
