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

import java.util.*;


public class Homework_1 {
    private static void MRPrintStatistics(JavaPairRDD<InputSet, Vector> universeSet, List<Vector> centerSet) {
        List<Tuple2<Integer, Tuple2<Integer, Integer>>> centerInfoList = universeSet.mapPartitions((partitions) -> {
            // for each partition compute (cluster_index, partial_count_na, partial_count_nb)
            List<Tuple3<Integer, Integer, Integer>> partialSum = new ArrayList<>(0);
            for (int i = 0; i < centerSet.size(); i++) {
                partialSum.add(new Tuple3<>(i, 0, 0));
            }
            partitions.forEachRemaining(tuple -> {
                int bestCenter = 0;
                double bestDist = Double.MAX_VALUE;
                for (int i = 0; i < centerSet.size(); i++) {
                    double dist = Vectors.sqdist(tuple._2, centerSet.get(i));
                    if (dist < bestDist) {
                        bestCenter = i;
                        bestDist = dist;
                    }
                }
                Tuple3<Integer, Integer, Integer> old = partialSum.get(bestCenter);
                if (tuple._1 == InputSet.SetA) {
                    partialSum.set(bestCenter, new Tuple3<>(bestCenter, old._2() + 1, old._3()));
                } else {
                    partialSum.set(bestCenter, new Tuple3<>(bestCenter, old._2(), old._3() + 1));
                }
            });
            return partialSum.iterator();
            // Group partial counts for every cluster and aggregate counts (cluster_index, count_na, count_nb)
        }).groupBy(Tuple3::_1).mapToPair((partial) -> {
            int totNa = 0;
            int totNb = 0;
            for (Tuple3<Integer, Integer, Integer> node : partial._2) {
                totNa += node._2();
                totNb += node._3();
            }
            return new Tuple2<>(partial._1, new Tuple2<>(totNa, totNb));
            // Sort to print clusters in asc order
        }).sortByKey().collect();

        centerInfoList.forEach(center -> {
            int center_index = center._1();
            long nA = center._2()._1();
            long nB = center._2()._2();
            Vector centerPos = centerSet.get(center_index);
            System.out.printf("i = %d, center = (%s), NA%d = %d, NB%d = %d\n",
                    center_index,
                    centerPos.toString(),
                    center_index,
                    nA,
                    center_index,
                    nB);
        });
    }

    private static double MRComputeStandardObjective(JavaRDD<Vector> points, List<Vector> centroids) {
        Tuple2<Double, Integer> total = points
                .mapPartitionsToPair(partition -> {
                    // (1, (sum_squared_distances, count))
                    double partition_cost = 0.0;
                    int partition_count = 0;

                    while (partition.hasNext()) {
                        Vector point = partition.next();
                        // Compute min squared distance from centroids
                        double cost = Double.POSITIVE_INFINITY;
                        for (Vector center : centroids) {
                            double distance = Vectors.sqdist(point, center);
                            cost = Math.min(distance, cost);
                        }
                        partition_cost += cost;
                        partition_count++;
                    }
                    return Collections.singletonList(new Tuple2<>(1, new Tuple2<>(partition_cost, partition_count))).iterator();
                }, true)
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

        MRPrintStatistics(inputPoints, Arrays.asList(clusters.clusterCenters()));
    }

    enum InputSet {
        SetA, SetB, Unknown
    }

}