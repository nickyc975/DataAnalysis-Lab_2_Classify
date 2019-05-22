import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Lab_2").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rawDataSet = sc.textFile("hdfs://localhost:9000/user/hduser/lab_2/input");
        System .out.println(rawDataSet.count());
    }
}
