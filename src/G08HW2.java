import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import scala.Tuple4;

import java.util.*;


public class G08HW2 {

    public static double[] computeVectorX(double fixedA, double fixedB, double[] alpha, double[] beta, double[] ell, int K) {
        double gamma = 0.5;
        double[] xDist = new double[K];
        double fA, fB;
        double power = 0.5;
        int T = 10;
        for (int t = 1; t <= T; t++) {
            fA = fixedA;
            fB = fixedB;
            power = power / 2;
            for (int i = 0; i < K; i++) {
                double temp = (1 - gamma) * beta[i] * ell[i] / (gamma * alpha[i] + (1 - gamma) * beta[i]);
                xDist[i] = temp;
                fA += alpha[i] * temp * temp;
                temp = (ell[i] - temp);
                fB += beta[i] * temp * temp;
            }
            if (fA == fB) {
                break;
            }
            gamma = (fA > fB) ? gamma + power : gamma - power;
        }
        return xDist;
    }

    // Computes the metrics required for Centroid selections i.e.
    // alpha
    // beta
    // mA
    // mB
    // l
    // nA
    // nB
    public static Metrics ComputeMetrics(JavaPairRDD<InputSet, Vector> universeSet, List<Vector> centerSet) {

        // This steps produces for each partition a list of tuples (cluster_index, (n_a, sum_a, n_b, sum_b))
        List<Tuple2<Integer, Tuple4<Integer, Vector, Integer, Vector>>> clusterMetrics = universeSet.mapPartitionsToPair(partition -> {
            List<Tuple2<Integer, Tuple4<Integer, Vector, Integer, Vector>>> partialSum = new ArrayList<>(0);
            // First add an entry for each cluster
            for (int i = 0; i < centerSet.size(); i++) {
                partialSum.add(
                        new Tuple2<>(
                                i,
                                new Tuple4<>(
                                        0,
                                        Vectors.zeros(centerSet.get(0).size()),
                                        0,
                                        Vectors.zeros(centerSet.get(0).size())
                                )
                        )
                );
            }
            // For every point in the partition compute the closest center, and its demographic,
            // then update the counters accordingly
            partition.forEachRemaining(point -> {
                int center = 0;
                double closest = Double.MAX_VALUE;
                for (int i = 0; i < centerSet.size(); i++) {
                    double dist = Vectors.sqdist(point._2, centerSet.get(i));
                    if (dist < closest) {
                        center = i;
                        closest = dist;
                    }
                }
                Tuple2<Integer, Tuple4<Integer, Vector, Integer, Vector>> old = partialSum.get(center);
                if (point._1 == InputSet.SetA) {
                    int newCount = old._2()._1() + 1;
                    Vector newSum = ExtendedVectors.sum(old._2._2(), point._2);
                    partialSum.set(center, new Tuple2<>(center, new Tuple4<>(newCount, newSum, old._2._3(), old._2._4())));
                } else {
                    int newCount = old._2()._3() + 1;
                    Vector newSum = ExtendedVectors.sum(old._2._4(), point._2);
                    partialSum.set(center, new Tuple2<>(center, new Tuple4<>(old._2._1(), old._2._2(), newCount, newSum)));
                }
            });
            return partialSum.iterator();
            // Here group by cluster index every partial count and aggregate counters
        }).reduceByKey((p1, p2) ->
                new Tuple4<>(
                        p1._1() + p2._1(),
                        ExtendedVectors.sum(p1._2(), p2._2()),
                        p1._3() + p2._3(),
                        ExtendedVectors.sum(p1._4(), p2._4())
                )
        ).collect();

        Metrics metrics = new Metrics(centerSet.size());
        for (Tuple2<Integer, Tuple4<Integer, Vector, Integer, Vector>> cluster : clusterMetrics) {
            metrics.append(cluster);
        }
        metrics.compute();
        return metrics;
    }

    // Computes the "contributions" for the fair k-means (aka. the "capital Delta" factors) = sum over all points in the
    // group of their quadratic distance to the centroid of their group, for both A and B.
    // Output is in shape (Delta_A, Delta_B)
    private static Tuple2<Double, Double> ComputeContributions(JavaPairRDD<InputSet, Vector> points, List<Vector> centroidsA, List<Vector> centroidsB) {
        List<Tuple2<InputSet, Double>> costs = points.mapPartitionsToPair(partition -> {
            double costA = 0.0;
            double costB = 0.0;
            while (partition.hasNext()) {
                Tuple2<InputSet, Vector> point = partition.next();
                double cost = Double.POSITIVE_INFINITY;
                List<Vector> centroids = (point._1 == InputSet.SetA) ? centroidsA : centroidsB;
                for (Vector center : centroids) {
                    double distance = Vectors.sqdist(point._2, center);
                    cost = Math.min(distance, cost);
                }
                if (point._1 == InputSet.SetA) {
                    costA += cost;
                } else {
                    costB += cost;
                }
            }

            List<Tuple2<InputSet, Double>> counts = new ArrayList<>();
            counts.add(new Tuple2<>(InputSet.SetA, costA));
            counts.add(new Tuple2<>(InputSet.SetB, costB));
            return counts.iterator();
        }).reduceByKey((c1, c2) -> c1 + c2).sortByKey().collect();

        return new Tuple2<>(costs.get(0)._2, costs.get(1)._2);
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

    private static Vector[] CentroidSelection(Vector[] stdCentrA, Vector[] stdCentrB, int k) {
        double fixedA = 0;
        double fixedB = 0;
        double[] alpha = new double[k];
        double[] beta = new double[k];
        double[] ell = new double[k];

        Vector[] c = new Vector[k];
        double[] x = computeVectorX(fixedA, fixedB, alpha, beta, ell, k);
        for (int i = 0; i < k; i++) {
            double[] stdCenterADigits = stdCentrA[i].toArray();
            double[] stdCenterBDigits = stdCentrB[i].toArray();
            double[] ciCoordinates = new double[stdCenterBDigits.length];
            double stdCenterAReduction = (ell[i] - x[i]);
            for (int j = 0; j < stdCenterBDigits.length; j++) { //TODO: parallelize the computation through map reduce
                ciCoordinates[j] = (stdCenterAReduction * stdCenterADigits[j] + x[i] * stdCenterBDigits[j]) / ell[i];
            }
            c[i] = Vectors.dense(ciCoordinates);
        }
        return c;
    }

    private static Vector[] MRFairLloyd(JavaPairRDD<InputSet, Vector> UniversePointSet, int K, int M) {
        Tuple2<Integer, Integer> ElementCounter = UniversePointSet
                .mapPartitions(partitions -> {
                    List<Tuple2<Integer, Integer>> partialSum = new ArrayList<>(2);

                    partialSum.set(0, new Tuple2<>(0, 0));
                    partialSum.set(1, new Tuple2<>(0, 0));

                    partitions.forEachRemaining(point -> {
                        if (point._1 == InputSet.SetA) {
                            Tuple2<Integer, Integer> updatedCount = new Tuple2<>(partialSum.get(0)._1 + 1, partialSum.get(1)._2);
                            partialSum.set(0, updatedCount);
                        } else {
                            Tuple2<Integer, Integer> updatedCount = new Tuple2<>(partialSum.get(0)._1, partialSum.get(1)._2 + 1);
                            partialSum.set(1, updatedCount);
                        }
                    });

                    return partialSum.iterator();
                })
                .reduce((counter, partitions) -> new Tuple2<>(counter._1 + partitions._1, counter._2 + partitions._2));
        //INFO: Initializes a set C of K centroids using kmeans||
        Vector[] C = KMeans.train(UniversePointSet.values().rdd(), K, 0).clusterCenters();

        for (int i = 0; i < M; i++) {
            Vector[] finalC = C;
            Vector[] standardCentrA = new Vector[K];
            Vector[] standardCentrB = new Vector[K];

            JavaPairRDD<Integer, Iterable<Tuple2<InputSet, Vector>>> partitions = UniversePointSet
                    .mapToPair(pair -> {
                        int c_i = 0;
                        double dist = Double.MAX_VALUE;

                        for (int j = 0; j < K; j++) {
                            double currDist = Vectors.sqdist(finalC[j], pair._2());
                            if (currDist < dist) {
                                c_i = j;
                                dist = currDist;
                            }
                        }

                        return new Tuple2<>(c_i, pair);
                    })
                    .sortByKey()
                    .groupByKey()
                    .cache();
//            C = CentroidSelection(partitions, ElementCounter._1, ElementCounter._2, standardCentrA, standardCentrB, K);
        }

        return C;
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

        JavaPairRDD<InputSet, Vector> inputPointsNoPartitions = sc.textFile(filename).mapToPair((line) -> {
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
        }).cache();

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

        // Computation of Standard Stats
        long startStandardKMeans = System.currentTimeMillis();
        List<Vector> Standardclusters = Arrays.asList(KMeans.train(strippedInputPoints.rdd(), K, M).clusterCenters());
        long endStandardKMeans = System.currentTimeMillis();
        long startStandardObjective = System.currentTimeMillis();
        double standardCost = MRComputeFairObjective(inputPoints, Standardclusters);
        long endStandardObjective = System.currentTimeMillis();

        // Computation of Fair Stats
        long startFairKMeans = System.currentTimeMillis();
        Vector[] Fairclusters = MRFairLloyd(inputPointsNoPartitions, K, M);
        long endFairKMeans = System.currentTimeMillis();
        long startFairObjective = System.currentTimeMillis();
        double fairCost = MRComputeFairObjective(inputPoints, Arrays.asList(Fairclusters));
        long endFairObjective = System.currentTimeMillis();

        //PRINT OBTAINED STATS
        //Standard
        System.out.printf("objective function output with standard Lloyd's algorithm :%d", standardCost);
        System.out.printf("time to compute standard KMeans: %d", (endStandardKMeans - startStandardKMeans) / 1000);
        System.out.printf("time to compute objective function with standard centroids: %d", (endStandardObjective - startStandardObjective) / 1000);

        //Fair
        System.out.printf("objective function output with fair Lloyd's algorithm :%d", fairCost);
        System.out.printf("time to compute fair KMeans: %d", (endFairKMeans - startFairKMeans) / 1000);
        System.out.printf("time to compute objective function with fair centroids: %d", (endFairObjective - startFairObjective) / 1000);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // STANDARD OBJECTIVE COST
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // double standard_cost = MRComputeStandardObjective(inputPoints.map(point -> point._2), Standardclusters);
        // System.out.printf("Delta(U, C) = %f\n", standard_cost);
        // System.out.printf("Phi(A, B, C) = %f\n", standardCost);

    }

    enum InputSet {
        SetA, SetB, Unknown
    }

    private static class ExtendedVectors {
        private static Vector sum(Vector v1, Vector v2) {
            double[] val1 = v1.toArray();
            double[] val2 = v2.toArray();
            double[] res = new double[val1.length];
            for (int i = 0; i < val1.length; i++) {
                res[i] += val1[i] + val2[i];
            }
            return Vectors.dense(res);
        }

        private static Vector sub(Vector v1, Vector v2) {
            return sum(v1, scale(v2, -1.0));
        }

        private static Vector scale(Vector v, double a) {
            return Vectors.dense(Arrays.stream(v.toArray()).map(x -> a * x).toArray());
        }
    }

    /*
     For all cluster compute
     - Ratio between the number of A elements in the cluster and the total number of elements in A
     - Ratio between the number of B elements in the cluster and the total number of elements in B
     - Average between all the vectors in the cluster in A
     - Average between all the vectors in the cluster in B
     - Euclidean distance between the two averages
     */
    static class Metrics {
        double[] alpha;
        double[] beta;
        Vector[] mA;
        Vector[] mB;
        double[] l;

        int nA;
        int nB;

        public Metrics(int k) {
            this.alpha = new double[k];
            this.beta = new double[k];
            this.mA = new Vector[k];
            this.mB = new Vector[k];
            this.l = new double[k];

            nA = 0;
            nB = 0;
        }

        public void append(Tuple2<Integer, Tuple4<Integer, Vector, Integer, Vector>> clusterMetric) {
            // Recall that tuple 4 === (count_a, sum_a, count_b, sum_b)
            alpha[clusterMetric._1] = clusterMetric._2._1();
            beta[clusterMetric._1] = clusterMetric._2._3();

            mA[clusterMetric._1] = ExtendedVectors.scale(clusterMetric._2._2(), 1.0 / clusterMetric._2._1());
            mB[clusterMetric._1] = ExtendedVectors.scale(clusterMetric._2._4(), 1.0 / clusterMetric._2._3());

            l[clusterMetric._1] = Vectors.norm(ExtendedVectors.sub(mA[clusterMetric._1], mB[clusterMetric._1]), 2);

            nA += clusterMetric._2._1();
            nB += clusterMetric._2._3();

        }

        public void compute() {
            for (int i = 0; i < alpha.length; i++) {
                alpha[i] /= nA;
            }
            for (int i = 0; i < beta.length; i++) {
                beta[i] /= nB;
            }
        }

    }

}
