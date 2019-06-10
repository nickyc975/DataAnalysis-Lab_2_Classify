import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    public static final String BASE_DIR = "hdfs://localhost:9000/user/hduser/lab_2";
    public static final String CLUSTER_INPUT = BASE_DIR + "/cluster/input";
    public static final String CLUSTER_OUTPUT = BASE_DIR + "/cluster/output";

    public static final String CLASSIFY_INPUT = BASE_DIR + "/classify/input";
    public static final String CLASSIFY_TRAIN = BASE_DIR + "/classify/train";
    public static final String CLASSIFY_TEST = BASE_DIR + "/classify/test";
    public static final String CLASSIFY_SAMPLE = BASE_DIR + "/classify/sample";
    public static final String CLASSIFY_OUTPUT = BASE_DIR + "/classify/output";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Lab_2")
                                        .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // KMeans(conf, sc);
        NaiveBayes(conf, sc);
        
        sc.close();
        sc.stop();
    }

    private static void KMeans(SparkConf conf, JavaSparkContext sc) {
        final int K = 10;
        final int ITER_COUNT = 15;
        final double MIN_DIFF = 1e-1;
        final String OUTPUT_DIR = CLUSTER_OUTPUT + "/kmeans";

        int iterCount = 0;
        double diff = Double.MAX_VALUE;
        List<Vector> newMeans, oldMeans;
        JavaPairRDD<Integer, Tuple2<Vector, Integer>> clustered;

        JavaRDD<Vector> vectors = sc.textFile(CLUSTER_INPUT).map(s -> {
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

        while (diff > MIN_DIFF && iterCount < ITER_COUNT) {
            clustered = vectors.mapToPair(clusterFunc);
            newMeans = clustered.reduceByKey((x, y) -> {
                           return new Tuple2<Vector,Integer>(Vector.add(x._1, y._1), x._2 + y._2);
                       }).values()
                       .map(value -> Vector.div(value._1, value._2))
                       .collect();
            conf.set("means", vectorsToString(newMeans));

            diff = 0;
            for (int i = 0; i < newMeans.size(); i++) {
                diff += Vector.sub(newMeans.get(i), oldMeans.get(i)).mod();
            }
            oldMeans = newMeans;
            iterCount++;

            conf.log().warn(String.format("K-Means: diff %f, iter count %d", diff, iterCount));
        }

        vectors.mapToPair(clusterFunc)
               .saveAsTextFile(OUTPUT_DIR);
    }

    @SuppressWarnings("unchecked")
    private static void NaiveBayes(SparkConf conf, JavaSparkContext sc) {
        JavaPairRDD<Integer, Vector> trainSet = sc.objectFile(CLASSIFY_TRAIN)
                                                  .mapToPair(obj -> (Tuple2<Integer, Vector>)obj);

        JavaPairRDD<Integer, Vector> testSet = sc.objectFile(CLASSIFY_TEST)
                                                  .mapToPair(obj -> (Tuple2<Integer, Vector>)obj);

        JavaPairRDD<Integer, Integer> prioriRDD = trainSet.mapToPair(
            vector -> new Tuple2<Integer, Integer>(vector._1, 1)
        ).reduceByKey((x, y) -> x + y);

        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Integer> posteriorRDD = trainSet.flatMapToPair(vector -> {
            List<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>> result = new ArrayList<>();
            for (int i = 0; i < vector._2.dim(); i++) {
                result.add(new Tuple2<Tuple3<Integer, Integer, Integer>, Integer>(
                    new Tuple3<>(vector._1, i, (int)vector._2.get(i)), 1
                ));
            }
            return result.iterator();
        }).reduceByKey((x, y) -> x + y);

        long total = trainSet.count();
        long cls_num = prioriRDD.keys().distinct().count();
        long dimension = posteriorRDD.keys().map(key -> key._2()).distinct().count();
        double[] priori = new double[(int)cls_num];
        double[][][] posterior = new double[(int)cls_num][(int)dimension][PreProcess.INTERVAL_NUM];

        for (Tuple2<Integer, Integer> tuple : prioriRDD.collect()) {
            priori[tuple._1] = tuple._2;
        }

        for (Tuple2<Tuple3<Integer, Integer, Integer>, Integer> tuple : posteriorRDD.collect()) {
            posterior[tuple._1._1()][tuple._1._2()][tuple._1._3()] = tuple._2 / priori[tuple._1._1()];
        }

        for (int i = 0; i < cls_num; i++) {
            priori[i] /= total;
        }

        int correct = testSet.map(value -> {
            int cls = 0;
            double maxProb = 0;
            for (int i = 0; i < cls_num; i++) {
                double prob = 0;
                for (int j = 0; j < dimension; j++) {
                    prob += priori[i] * posterior[i][j][(int)value._2.get(j)];
                }
                if (prob > maxProb) {
                    cls = i;
                    maxProb = prob;
                }
            }
            return value._1 == cls ? 1 : 0;
        }).reduce((x, y) -> x + y);
        double correctness = (double)correct / (double)testSet.count();
        conf.log().warn(String.format("correct: %d, total: %d, correctness: %f", correct, testSet.count(), correctness));
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
