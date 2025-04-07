import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class Homework_1 {
    private static <T> void MRPrintStatistics(JavaRDD<Tuple2<InputSet, T>> universeSet, List<Vector> centerSet) {
        AtomicInteger centerIndex = new AtomicInteger(0);
        centerSet.forEach((c) -> {
            JavaRDD<Tuple2<Long, Long>> belongCount = universeSet.map(t -> {
                if (t._2.equals(c)) {
                    switch (t._1) {
                        case SetA:
                            return new Tuple2(1L, 0L);
                        case SetB:
                            return new Tuple2(0L, 1L);
                    }
                }
                return new Tuple2(0L, 0L);
            });
            Tuple2<Long, Long> countResult = belongCount.reduce((a, b) -> {
                return new Tuple2(a._1 + b._1, a._2 + b._2);
            });
            System.out.printf("i = %d, ", centerIndex.get());
            System.out.printf("center = (%s), ", c.toString());
            System.out.printf("NA%d = %d, ", centerIndex.get(), countResult._1);
            System.out.printf("NB%d = %d\n", centerIndex.get(), countResult._2);
            centerIndex.addAndGet(1);
        });

    }

    private static double MRComputeStandardObjective(JavaRDD<Vector> points, List<Vector> centroids) {
        Tuple2<Double, Integer> total = points
                .mapPartitionsWithIndex((index, partition) -> {
                    ArrayList<Tuple3<Integer, Double, Integer>> distances = new ArrayList<>();
                    partition.forEachRemaining(point -> {
                        // Compute min squared distance from centroids
                        double cost = Double.POSITIVE_INFINITY;
                        for (Vector center : centroids) {
                            double distance = Vectors.sqdist(point, center);
                            cost = Math.min(distance, cost);
                        }
                        distances.add(new Tuple3<>(index, cost, 1));
                    });
                    return distances.iterator();
                }, true)
                .groupBy(Tuple3::_1)
                .mapToPair((partials) -> {
                    Iterator<Tuple3<Integer, Double, Integer>> it = partials._2.iterator();
                    double cost = 0.0;
                    int count = 0;
                    while (it.hasNext()) {
                        Tuple3<Integer, Double, Integer> partial = it.next();
                        cost += partial._2();
                        count += partial._3();
                    }
                    return new Tuple2<>(1, new Tuple2<>(cost, count));
                })
                .reduceByKey((p1, p2) -> new Tuple2<>(p1._1 + p2._1, p1._2 + p2._2))
                .collectAsMap()
                .get(1);

        return total._1 / total._2;

    }

    private static double MRComputeFairObjective(JavaPairRDD<InputSet, Vector> points, List<Vector> centroids) {
        // First, separate points by InputSet (A and B)
        JavaRDD<Vector> setAPoints = points
                .filter(point -> point._1 == InputSet.SetA)
                .map(point -> point._2);

        JavaRDD<Vector> setBPoints = points
                .filter(point -> point._1 == InputSet.SetB)
                .map(point -> point._2);

        // Compute objective for set A
        double objA = MRComputeStandardObjective(setAPoints, centroids);

        // Compute objective for set B
        double objB = MRComputeStandardObjective(setBPoints, centroids);

        // Return maximum of the two objectives (fair objective)
        return Math.max(objA, objB);
    }

    public static void main(String[] args) {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: num_partitions, <path_to_file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: num_partitions num_clusters max_iterations");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        Logger.getLogger("org.spark-project").setLevel(Level.ERROR);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf(true).setAppName("Homework1");
        int K;
        int M;
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions and filename
        String filename = args[0];
        int L = Integer.parseInt(args[1]);
        K = Integer.parseInt(args[2]);
        M = Integer.parseInt(args[3]);

        System.out.printf("Input file = %s, L = %d, K = %d, M = %d\n", filename, L, K, M);

        // Read input file, parse content and subdivide it into L random partitions
        JavaPairRDD<InputSet, Vector> inputPoints = sc.textFile(filename).mapToPair((line) -> {
            ArrayList<Double> entries = new ArrayList<>();
            InputSet set = InputSet.Unknown;
            Iterator<String> tokens = Arrays.stream(line.split(",")).iterator();
            while (tokens.hasNext()) {
                String token = tokens.next();
                if (tokens.hasNext()) {
                    entries.add(Double.parseDouble(token));
                } else {
                    switch (token) {
                        case "A":
                            set = InputSet.SetA;
                            break;
                        case "B":
                            set = InputSet.SetB;
                            break;
                        default:
                            set = InputSet.Unknown;
                    }
                }
            }
            return new Tuple2<>(set, Vectors.dense(entries.stream().mapToDouble(Double::doubleValue).toArray()));
        }).repartition(L).cache();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // PRINT NUMBER OF POINTS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Count total points
        long totalCount = inputPoints.count();
        // Count points in Set A
        long countA = inputPoints.filter(point -> point._1 == InputSet.SetA).count();
        // Count points in Set B
        long countB = inputPoints.filter(point -> point._1 == InputSet.SetB).count();
        System.out.printf("N = %d, NA = %d, NB = %d\n", totalCount, countA, countB);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // K-MEANS CLUSTERING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Strip class information from original dataset
        JavaRDD<Vector> strippedInputPoints = inputPoints.mapPartitions((points) -> {
            ArrayList<Vector> strippedPoints = new ArrayList<>();
            while (points.hasNext()) {
                strippedPoints.add(points.next()._2);
            }
            return strippedPoints.iterator();
        }, true).cache();

        // Cluster the data into two classes using KMeans
        KMeansModel clusters = KMeans.train(strippedInputPoints.rdd(), K, M);


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // STANDARD OBJECTIVE COST
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        double standard_cost = MRComputeStandardObjective(inputPoints.map(point -> point._2), Arrays.asList(clusters.clusterCenters()));
        System.out.printf("Delta(U, C) = %f\n", standard_cost);
        double fair_cost = MRComputeFairObjective(inputPoints, Arrays.asList(clusters.clusterCenters()));
        System.out.printf("Phi(A, B, C) = %f\n", fair_cost);

//        MRPrintStatistics(inputPoints, Arrays.asList(clusters.clusterCenters()));
    }

    enum InputSet {
        SetA, SetB, Unknown
    }

}