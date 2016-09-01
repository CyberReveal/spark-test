package com.baesystems.sparktest;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Class: Summarise
 */
public final class Summarise {

    public static class Stats implements Serializable {

        private final int count;
        private final int numBytes;

        public Stats(final int count, final int numBytes) {
            this.count = count;
            this.numBytes = numBytes;
        }

        public Stats merge(final Stats other) {
            return new Stats(this.count + other.count, this.numBytes + other.numBytes);
        }

        @Override
        public String toString() {
            return String.format("bytes=%s\tcount=%s", this.numBytes, this.count);
        }
    }

    public static Tuple3<String, String, String> extractKey(final String line) {
        Matcher m = doMatching(line);
        if (m == null) {
            return new Tuple3<>(null, null, null);
        }

        String ip = m.group(1);
        String user = m.group(3);
        String query = m.group(5);
        return new Tuple3<>(ip, user, query);
    }

    public static Stats extractStats(final String line) {
        Matcher m = doMatching(line);
        if (m != null) {
            try {
                int bytes = Integer.parseInt(m.group(9));
                return new Stats(1, bytes);
            } catch (java.lang.NumberFormatException e) {
            }
        }
        return new Stats(1, 0);
    }

    public static Matcher doMatching(final String line) {
        Pattern logRegex = Pattern.compile(
                "^(\\S+) (\\S+) (\\S+) \\[(.*)] \"(\\S+) (\\S+) (\\S+)\" (\\d*) (\\d*).?");
        Matcher m = logRegex.matcher(line);
        if (m.find()) {
            return m;
        } else {
            return null;
        }
    }

    /**
     * The main method
     *
     * @param args
     */
    public static void main(final String[] args) {

        JavaSparkContext sc = new JavaSparkContext();

        JavaRDD<String> dataSet = (args.length == 1) ? sc.textFile(args[0]) : sc.textFile("hdfs:/access_log.txt");

        JavaPairRDD<Tuple3<String, String, String>, Stats> extracted = dataSet
                .mapToPair(new PairFunction<String, Tuple3<String, String, String>, Stats>() {
                    @Override
                    public Tuple2<Tuple3<String, String, String>, Stats> call(final String s) {
                        return new Tuple2<>(extractKey(s), extractStats(s));
                    }
                });

        JavaPairRDD<Tuple3<String, String, String>, Stats> counts = extracted
                .reduceByKey(new Function2<Stats, Stats, Stats>() {
                    @Override
                    public Stats call(final Stats stats, final Stats stats2) {
                        return stats.merge(stats2);
                    }
                });

        int counter = 0;
        List<Tuple2<Tuple3<String, String, String>, Stats>> output = counts.collect();
        for (Tuple2<?, ?> t : output) {
            System.out.println(t._1() + "\t" + t._2());
            counter += ((Stats) t._2()).count;
        }
        System.out.println("Total rows: " + counter);
    }
}
