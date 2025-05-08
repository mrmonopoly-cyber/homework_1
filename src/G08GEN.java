import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Locale;
import java.util.Random;

public class G08GEN {

    public static void main(String[] args) {

        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: num_points num_clusters");
        }

        // parse N, K
        final int N = Integer.parseInt(args[0]);
        final int K = Integer.parseInt(args[1]);

        // parameters
        final double BLOB_RATIO = 0.95;
        final double BLOB_RADIUS = 30.0;
        final double BLOB_PA = 0.98;
        final double SATELLITES_DISTANCE = 40.0;
        final double SATELLITES_RADIUS = 2.0;
        final double SATELLITES_PA = 0.02;

        // First create a big blob in the center
        int blobPoints = (int)Math.ceil(N * BLOB_RATIO);
        for (int i = 0; i < blobPoints; i++) {
            RandomPoint p = new RandomPoint(BLOB_RADIUS, 0.0, 0.0, BLOB_PA);
            System.out.println(p);
        }

        // For every satellite
        int satellitesPoints = N - blobPoints;
        for (int k = 0; k < K; k++) {
            // Compute points per satellite
            int satellitePoints = satellitesPoints / K +
                    ((k < (satellitesPoints % K)) ? 1 : 0);

            // Compute angle
            double angle = (2 * Math.PI / K) * k;
            double x = SATELLITES_DISTANCE * Math.cos(angle);
            double y = SATELLITES_DISTANCE * Math.sin(angle);

            for (int i = 0; i < satellitePoints; i++) {
                RandomPoint p = new RandomPoint(SATELLITES_RADIUS, x, y, SATELLITES_PA);
                System.out.println(p);
            }
        }
    }

    enum Demographic {
        A, B
    }

    static class RandomPoint {
        public Vector vector;
        public Demographic demographic;

        public RandomPoint(double scale, double xOffset, double yOffset, double pA) {
            double angle = 2 * Math.PI * Math.random();
            double mod = scale * Math.random();
            double x = mod * Math.cos(angle) + xOffset;
            double y = mod * Math.sin(angle) + yOffset;
            vector = Vectors.dense(new double[]{x, y});
            demographic = (Math.random() < pA) ? Demographic.A : Demographic.B;
        }

        @Override
        public String toString() {
            String s = "";
            for (double component : vector.toArray()) {
                s +=  String.format(Locale.ENGLISH, "%.4f", component) + ",";
            }
            s += (demographic == Demographic.A) ? "A" : "B";
            return s;
        }
    }
}
