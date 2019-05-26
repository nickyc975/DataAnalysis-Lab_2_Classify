import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Lab_2").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rawDataSet = sc.textFile("hdfs://localhost:9000/user/hduser/lab_2/input");
        JavaRDD<Vector> vectors = rawDataSet.map(s -> {
            String[] parts = s.split(", *");
            double[] vector = new double[parts.length];
            for (int i = 0; i < parts.length; i++) {
                vector[i] = Double.parseDouble(parts[i]);
            }
            return Vectors.dense(vector);
        });
    }
}
