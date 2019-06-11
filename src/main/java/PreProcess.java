import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PreProcess {
    public static final int INTERVAL_NUM = 64;
    public static final double SAMPLE_RATIO = 1e-1;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Lab_2_pre_process").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        conf.log().info("Reading...");
        JavaPairRDD<Integer, Vector> trainSet = sc.textFile(Main.CLASSIFY_INPUT + "/train").mapToPair(s -> {
            String[] parts = s.split(", *");
            double[] vector = new double[parts.length - 1];
            for (int i = 1; i < parts.length; i++) {
                vector[i - 1] = Double.parseDouble(parts[i]);
            }
            return new Tuple2<>((int) Double.parseDouble(parts[0]), new Vector(vector));
        });

        JavaPairRDD<Integer, Vector> testSet = sc.textFile(Main.CLASSIFY_INPUT + "/test").mapToPair(s -> {
            String[] parts = s.split(", *");
            double[] vector = new double[parts.length - 1];
            for (int i = 1; i < parts.length; i++) {
                vector[i - 1] = Double.parseDouble(parts[i]);
            }
            return new Tuple2<>((int) Double.parseDouble(parts[0]), new Vector(vector));
        });

        conf.log().info("Calculating splits...");
        JavaPairRDD<Integer, Vector> sample = trainSet.sample(false, SAMPLE_RATIO);

        long sampleCount = sample.count();
        long step = sampleCount / INTERVAL_NUM;
        List<Tuple2<Integer, Tuple2<Integer, Double>>> splitList = sample
        .zipWithIndex().flatMap(tuple -> {
            Vector vector = tuple._1._2;
            List<Tuple2<Integer, Double>> result = new ArrayList<>();
            for (int row = 0; row < vector.dim(); row++) {
                result.add(new Tuple2<>(row, vector.get(row)));
            }
            return result.iterator();
        }).sortBy(ComparableTuple2::new, true, trainSet.getNumPartitions())
        .zipWithIndex().filter(tuple -> {
            long index = tuple._2 % sampleCount;
            return index >= step && index <= (sampleCount - step) && index % step == 0;
        }).mapToPair(tuple -> {
            int col = tuple._1._1;
            long row = (tuple._2 % sampleCount) / step - 1;
            return new Tuple2<Integer, Tuple2<Integer, Double>>((int) row, new Tuple2<>(col, tuple._1._2));
        }).collect();

        int dimension = trainSet.take(1).get(0)._2.dim();
        double[][] splits = new double[INTERVAL_NUM - 1][dimension];
        for (Tuple2<Integer, Tuple2<Integer, Double>> tuple : splitList) {
            splits[tuple._1][tuple._2._1] = tuple._2._2;
        }

        conf.log().info("Discreting...");
        PairFunction<Tuple2<Integer, Vector>, Integer, Vector> discrete = vector -> {
            Vector result = new Vector(vector._2.dim(), 0);
            for (int i = 0; i < result.dim(); i++) {
                int value = 0;
                while (value < splits.length && splits[value][i] < vector._2.get(i)) {
                    value++;
                }
                result.set(i, value);
            }
            return new Tuple2<>(vector._1, result);
        };
        trainSet.mapToPair(discrete).saveAsObjectFile(Main.CLASSIFY_TRAIN);
        testSet.mapToPair(discrete).saveAsObjectFile(Main.CLASSIFY_TEST);

        sc.close();
        sc.stop();
    }
}