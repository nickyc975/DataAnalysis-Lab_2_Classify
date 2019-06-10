import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PreProcess {
    public static final int INTERVAL_NUM = 64;
    public static final double SAMPLE_RATIO = 1e-1;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Lab_2_pre_process")
                                        .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        conf.log().info("Reading...");
        JavaPairRDD<Integer, Vector> trainSet = sc.textFile(Main.CLASSIFY_INPUT + "/train").mapToPair(s -> {
            String[] parts = s.split(", *");
            double[] vector = new double[parts.length - 1];
            for (int i = 1; i < parts.length; i++) {
                vector[i - 1] = Double.parseDouble(parts[i]);
            }
            return new Tuple2<>((int)Double.parseDouble(parts[0]), new Vector(vector));
        });

        JavaPairRDD<Integer, Vector> testSet = sc.textFile(Main.CLASSIFY_INPUT + "/test").mapToPair(s -> {
            String[] parts = s.split(", *");
            double[] vector = new double[parts.length - 1];
            for (int i = 1; i < parts.length; i++) {
                vector[i - 1] = Double.parseDouble(parts[i]);
            }
            return new Tuple2<>((int)Double.parseDouble(parts[0]), new Vector(vector));
        });

        List<Vector> sample = trainSet.sample(false, SAMPLE_RATIO).map(value -> value._2).collect();

        conf.log().info("Calculating splits...");
        int dimension = trainSet.take(1).get(0)._2.dim();
        Vector[] splits = new Vector[INTERVAL_NUM - 1];
        for (int i = 0; i < splits.length; i++) {
            splits[i] = new Vector(dimension, Double.MAX_VALUE);
        }

        List<List<Double>> trans = new ArrayList<>();
        for (int i = 0; i < dimension; i++) {
            trans.add(new ArrayList<>());
            for (int j = 0; j < sample.size(); j++) {
                trans.get(i).add(sample.get(j).get(i));
            }
            conf.log().info("Transed dimension: " + i);
        }

        int step = sample.size() / INTERVAL_NUM;
        for (int i = 0; i < dimension; i++) {
            int index = 1;
            conf.log().info("Sorting dimension: " + i);
            Collections.sort(trans.get(i), Comparator.comparingDouble(v -> v));
            conf.log().info("Sorted dimension: " + i);
            while (index < INTERVAL_NUM) {
                splits[index - 1].set(i, trans.get(i).get(index * step));
                index++;
            }
        }

        conf.log().info("Discreting...");
        trainSet.mapToPair(vector -> {
            Vector result = new Vector(vector._2.dim(), 0);
            for (int i = 0; i < result.dim(); i++) {
                int value = 0;
                while (value < splits.length && splits[value].get(i) < vector._2.get(i)) {
                    value++;
                }
                result.set(i, value);
            }
            return new Tuple2<>(vector._1, result);
        }).saveAsObjectFile(Main.CLASSIFY_TRAIN);

        testSet.mapToPair(vector -> {
            Vector result = new Vector(vector._2.dim(), 0);
            for (int i = 0; i < result.dim(); i++) {
                int value = 0;
                while (value < splits.length && splits[value].get(i) < vector._2.get(i)) {
                    value++;
                }
                result.set(i, value);
            }
            return new Tuple2<>(vector._1, result);
        }).saveAsObjectFile(Main.CLASSIFY_TEST);
        
        sc.close();
        sc.stop();
    }
}