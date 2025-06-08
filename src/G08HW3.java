import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.App;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class G08HW3 {

    private static List<Tuple2<Long,Long>> topKHeavyHitter(List<Tuple2<Long,Long>> trueFreqList, int k) {
        ArrayList<Tuple2<Long,Long>> res = new ArrayList<>(0);
        Long phi_k = trueFreqList.get(k)._2();
        int i=0;
        for (Tuple2<Long,Long> point : trueFreqList){
            if (point._2()>= phi_k)
            {
                res.add(point);
                i++;
            }
            if (i>=k) {
                break;
            }
        }

        return res;
    }

    private static Long frequencyRelativeError(List<Tuple2<Long,Long >> trueFrequencies, List<Tuple2<Long,Long>> estimatedFrequencies, int k) {
        List<Tuple2<Long,Long>> topKHeavyHitters = topKHeavyHitter(trueFrequencies,k);
        SparkConf sparkConf = new SparkConf().setAppName("FindKTopApprox").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Long errorSum = sparkContext.parallelize(estimatedFrequencies)
                .repartition(k)
                .map( (point) -> {
                    List<Tuple2<Long, Long>> eleMaybe = topKHeavyHitters
                            .stream()
                            .filter((a) -> a._1().equals(point._1()))
                            .collect(Collectors.toList());
                    if (eleMaybe.size()!=0)
                    {
                        Tuple2<Long,Long> topHitter = eleMaybe.get(0);
                        return Math.abs(topHitter._2() - point._2())/topHitter._2();
                    }else {
                        return new Long(0);
                    }
                })
                .reduce(Long::sum);

        return errorSum/k;
    }

    private static List<Tuple2<Long,Long>> CountMin(List<Long> u, int d, int w) {
        List<Tuple2<Long,Long>> res = new ArrayList<>();
        return res;
    }

    private static List<Tuple2<Long,Long>> CountSketch(List<Long> u, int d, int w) {
        List<Tuple2<Long,Long>> res = new ArrayList<>();
        return res;
    }

    // After how many items should we stop?
    // public static final int THRESHOLD = 1000000;
    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: port, threshold, D, W, K");
        }
        // IMPORTANT: the master must be set to "local[*]" or "local[n]" with n > 1, otherwise
        // there will be no processor running the streaming computation and your
        // code will crash with an out of memory (because the input keeps accumulating).
        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]") // remove this line if running on the cluster
                .setAppName("DistinctExample");

        // The definition of the streaming spark context  below, specifies the amount of
        // time used for collecting a batch, hence giving some control on the batch size.
        // Beware that the data generator we are using is very fast, so the suggestion is to
        // use batches of less than a second, otherwise you might exhaust the JVM memory.
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        sc.sparkContext().setLogLevel("ERROR");

        // TECHNICAL DETAIL:
        // The streaming spark context and our code and the tasks that are spawned all
        // work concurrently. To ensure a clean shut down we use this semaphore. The 
        // main thread will first acquire the only permit available, and then it will try
        // to acquire another one right after spinning up the streaming computation.
        // The second attempt at acquiring the semaphore will make the main thread
        // wait on the call. Then, in the `foreachRDD` call, when the stopping condition
        // is met the semaphore is released, basically giving "green light" to the main
        // thread to shut down the computation. We cannot call `sc.stop()` directly in `foreachRDD`
        // because it might lead to deadlocks.

        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        int P = Integer.parseInt(args[0]);
        int T = Integer.parseInt(args[1]);
        int D = Integer.parseInt(args[2]);
        int W = Integer.parseInt(args[3]);
        int K = Integer.parseInt(args[4]);

        System.out.printf("Port = %d T = %d D = %d W = %d K = %d\n", P, T, D, W, K);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Variable streamLength below is used to maintain the number of processed stream items.
        // It must be defined as a 1-element array so that the value stored into the array can be
        // changed within the lambda used in foreachRDD. Using a simple external counter streamLength of type
        // long would not work since the lambda would not be allowed to update it.
        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        streamLength[0] = 0L;
        HashMap<Long, Long> histogram = new HashMap<>(); // Hash Table for the distinct elements

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", P, StorageLevels.MEMORY_AND_DISK)
                // For each batch, to the following.
                // BEWARE: the `foreachRDD` method has "at least once semantics", meaning
                // that the same data might be processed multiple times in case of failure.
                .foreachRDD((batch, time) -> {
                    // this is working on the batch at time `time`.
                    if (streamLength[0] < T) {
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;
                        if (batchSize > 0) {
                            System.out.println("Batch size at time [" + time + "] is: " + batchSize);
                            // Extract the distinct items from the batch
                            Map<Long, Long> batchItems = batch
                                    .mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
                                    .reduceByKey((i1, i2) -> 1L)
                                    .collectAsMap();
                            // Update the streaming state. If the overall count of processed items reaches the
                            // THRESHOLD value (among all batches processed so far), subsequent items of the
                            // current batch are ignored, and no further batches will be processed
                            for (Map.Entry<Long, Long> pair : batchItems.entrySet()) {
                                if (!histogram.containsKey(pair.getKey())) {
                                    histogram.put(pair.getKey(), 1L);
                                }
                            }
                            // If we wanted, here we could run some additional code on the global histogram
                            if (streamLength[0] >= T) {
                                // Stop receiving and processing further batches
                                stoppingSemaphore.release();
                            }

                        }
                    }
                });

        // MANAGING STREAMING SPARK CONTEXT
        System.out.println("Starting streaming engine");
        sc.start();
        System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        System.out.println("Stopping the streaming engine");

        /* The following command stops the execution of the stream. The first boolean, if true, also
           stops the SparkContext, while the second boolean, if true, stops gracefully by waiting for
           the processing of all received data to be completed. You might get some error messages when
           the program ends, but they will not affect the correctness. You may also try to set the second
           parameter to true.
        */


        sc.stop(false, false);
        //System.out.println("Streaming engine stopped");

        // COMPUTE AND PRINT FINAL STATISTICS
        System.out.println("Number of processed items = " + streamLength[0]);
        System.out.println("Number of distinct items = " + histogram.size());
        long max = 0L;
        ArrayList<Long> distinctKeys = new ArrayList<>(histogram.keySet());

        Collections.sort(distinctKeys, Collections.reverseOrder());

        Comparator<? super Tuple2<Long,Long>> CompPointFreq = new Comparator<Tuple2<Long,Long>>() {
            @Override
            public int compare(Tuple2<Long, Long> o1, Tuple2<Long, Long> o2) {
                return (int) -(o1._2() - o2._2());
            }
        };
        List<Tuple2<Long,Long>> trueFrequencies = CountMin(distinctKeys,histogram.size(),histogram.size());
        trueFrequencies.sort(CompPointFreq);
        List<Tuple2<Long,Long>> cmFrequencies = CountMin(distinctKeys,K,K);
        List<Tuple2<Long,Long>> csFrequencies = CountMin(distinctKeys,K,K);

        float errorEstimationCM = frequencyRelativeError(trueFrequencies, cmFrequencies, K);
        float errorEstimationCS = frequencyRelativeError(trueFrequencies, csFrequencies, K);
        
        // Number of Top-K Heavy Hitters = 30
        // Avg Relative Error for Top-K Heavy Hitters with CM = 137.00788548942958
        // Avg Relative Error for Top-K Heavy Hitters with CS = 2.3336001311237653

        List<Tuple2<Long,Long>> topKHeavyHitter = topKHeavyHitter(trueFrequencies,K);
        System.out.printf("Number of Top-K Heavy Hitters = %d\n", topKHeavyHitter.size());
        System.out.printf("Avg Relative Error for Top-K Heavy Hitters with CM = %f\n", errorEstimationCM);
        System.out.printf("Avg Relative Error for Top-K Heavy Hitters with CS = %f\n", errorEstimationCS);

        //Item 195773912 True Frequency = 32311 Estimated Frequency with CM = 33095
        //Item 339323283 True Frequency = 32142 Estimated Frequency with CM = 32879
        //Item 434415286 True Frequency = 31953 Estimated Frequency with CM = 32714
        //Item 641486445 True Frequency = 32118 Estimated Frequency with CM = 32875
        //Item 819911327 True Frequency = 32371 Estimated Frequency with CM = 33133
        //Item 870070186 True Frequency = 32160 Estimated Frequency with CM = 32925
        //Item 1472610405 True Frequency = 32255 Estimated Frequency with CM = 33028
        //Item 1590293530 True Frequency = 31771 Estimated Frequency with CM = 32541
        //Item 1690049656 True Frequency = 32362 Estimated Frequency with CM = 33159
        //Item 1936875793 True Frequency = 32286 Estimated Frequency with CM = 33091

        Comparator<? super Tuple2<Long,Long>> topKHeavyHitterSort= new Comparator<Tuple2<Long,Long>>() {
            @Override
            public int compare(Tuple2<Long, Long> o1, Tuple2<Long, Long> o2) {
                return o1._1().compareTo(o2._1());
            }
        };

        topKHeavyHitter.sort(topKHeavyHitterSort);
        if (topKHeavyHitter.size() <=10) {
            for (Tuple2<Long,Long> point : topKHeavyHitter) {
                Long cmFreq = cmFrequencies.
                        stream().
                        filter(a -> a._1().equals(point._1()))
                        .collect(Collectors.toList())
                        .get(0)
                        ._2();

                System.out.printf("Item %d True Frequency = %d Estimated Frequency with CM = %d",
                        point._1(), point._2(), cmFreq);
                
            }
        }


        //System.out.println("Largest item = " + distinctKeys.get(0));
    }
}
