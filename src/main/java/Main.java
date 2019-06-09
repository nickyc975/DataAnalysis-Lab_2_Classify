import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    private static final int K = 3;
    private static final int ITER_COUNT = 20;
    private static final double MIN_DIFF = 1e-2;
    private static final String INPUT_DIR = "hdfs://localhost:9000/user/hduser/lab_2/input";
    private static final String OUTPUT_DIR = "hdfs://localhost:9000/user/hduser/lab_2/output";

    public static void main(String[] args) {
        List<Vector> newMeans, oldMeans;
        JavaPairRDD<Integer, Tuple2<Vector, Integer>> clustered;
        SparkConf conf = new SparkConf().setAppName("Lab_2").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Vector> vectors = sc.textFile(INPUT_DIR).map(s -> {
            String[] parts = s.split(", *");
            double[] vector = new double[parts.length - 1];
            for (int i = 1; i < parts.length; i++) {
                vector[i - 1] = Double.parseDouble(parts[i]);
            }
            return new Vector(vector);
        });

        oldMeans = vectors.takeSample(false, K);
        conf.set("means", vectorsToString(oldMeans));

        PairFunction<Vector, Integer, Tuple2<Vector, Integer>> clusterFunc = vector -> {
            List<Vector> means = stringToVectors(conf.get("means"));
            int belongsTo = 0;
            double distance, minDistance = Double.MAX_VALUE;
            for (int i = 0; i < means.size(); i++) {
                distance = Vector.sub(means.get(i), vector).mod();
                if (distance < minDistance) {
                    belongsTo = i;
                    minDistance = distance;
                }
            }
            return new Tuple2<>(belongsTo, new Tuple2<>(vector, 1));
        };

        int iterCount = 0;
        double diff = Double.MAX_VALUE;
        while (diff > MIN_DIFF && iterCount < ITER_COUNT) {
            clustered = vectors.mapToPair(clusterFunc);
            newMeans = clustered.reduceByKey((x, y) -> {
                           return new Tuple2<Vector,Integer>(Vector.add(x._1(), y._1()), x._2() + y._2());
                       }).values()
                       .map(value -> Vector.div(value._1(), value._2()))
                       .collect();
            conf.set("means", vectorsToString(newMeans));

            diff = 0;
            for (int i = 0; i < newMeans.size(); i++) {
                diff += Vector.sub(newMeans.get(i), oldMeans.get(i)).mod();
            }
            oldMeans = newMeans;
            iterCount++;
        }

        vectors.mapToPair(clusterFunc)
               .saveAsTextFile(OUTPUT_DIR);
        sc.close();
        sc.stop();
    }

    private static String vectorsToString(List<Vector> vectors) {
        return vectors.stream()
                      .map(Vector::toString)
                      .reduce((s1, s2) -> s1 + "\n" + s2)
                      .orElse("");
    }

    private static List<Vector> stringToVectors(String str) {
        return Arrays.asList(str.split("\n"))
                     .stream()
                     .map(Vector::fromString)
                     .collect(Collectors.toList());
    }
}
