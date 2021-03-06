import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PageRank {

    private static final int NUM_OF_PAGE_RANK_ITERATES = 42;
    private static final double DUMPING_FACTOR = .85;

    private static final byte[] CF_COLUMN_FAMILY = "cf".getBytes();
    private static final byte[] SL_COLUMN_FAMILY = "sl".getBytes();
    private static final byte[] PAGE_RANK = "pr".getBytes();
    private static final byte[] SUB_LINKS_COLUMN = "subLinks".getBytes();

    public static void main(String[] args) throws Exception {
        final SparkConf sparkConf = new SparkConf().setAppName("PageRank").setMaster("spark://master:7077");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        Configuration hbaseConfiguration = HBaseConfiguration.create();

        hbaseConfiguration.set("hbase.master", "master:60000");
        hbaseConfiguration.set("zookeeper.znode.parent", "/hbase");
        hbaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConfiguration.set("hbase.zookeeper.quorum", "master,slave");
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, "wb");

        Job newAPIJobConfiguration = Job.getInstance(hbaseConfiguration);
        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "wb");
        newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

        JavaPairRDD<ImmutableBytesWritable, Result> hbaseData = javaSparkContext.newAPIHadoopRDD(
                hbaseConfiguration,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class);

        JavaPairRDD<String, ArrayList<String>> webGraph = hbaseData.mapToPair(
                new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, ArrayList<String>>() {
                    public Tuple2<String, ArrayList<String>> call(Tuple2<ImmutableBytesWritable,
                                                                  Result> tuple2) throws Exception {
                        String subLinks = null;
                        try {
                            subLinks = new String(tuple2._2.getValue(SL_COLUMN_FAMILY, SUB_LINKS_COLUMN));
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        ArrayList<String> outputLinks = new ArrayList<String>();

                        if(subLinks != null) {
                            String[] urls = subLinks.split("\n");
                            for(int i = 0; i < urls.length; i += 2) {
                                outputLinks.add(urls[i]);
                            }
                        }

                        return new Tuple2<String, ArrayList<String>>(new String(tuple2._1.copyBytes()), outputLinks);
                    }
                }
        );

        JavaPairRDD<String, Double> pageRanks = webGraph.mapValues(
                new Function<ArrayList<String>, Double>() {
                    public Double call(ArrayList<String> v1) throws Exception {
                        return 1.;
                    }
                }
        );

        for(int it = 0; it < NUM_OF_PAGE_RANK_ITERATES; it++) {
            JavaPairRDD<String, Double> contributions = webGraph.join(pageRanks).flatMapToPair(
                    new PairFlatMapFunction<Tuple2<String, Tuple2<ArrayList<String>, Double>>, String, Double>() {
                        public Iterator<Tuple2<String, Double>>
                        call(Tuple2<String, Tuple2<ArrayList<String>, Double>> stringTuple2Tuple2) throws Exception {
                            int outputLinkCount = Iterables.size(stringTuple2Tuple2._2._1);

                            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();

                            //handle convergent
                            results.add(new Tuple2<String, Double>(stringTuple2Tuple2._1, 0.));

                            for(String outputLink : stringTuple2Tuple2._2._1) {
                                results.add(new Tuple2<String, Double>(
                                        outputLink,
                                        stringTuple2Tuple2._2._2 / outputLinkCount));
                            }

                            return results.iterator();
                        }
                    }
            );

            pageRanks = contributions
                    .reduceByKey(
                    new Function2<Double, Double, Double>() {
                        public Double call(Double v1, Double v2) throws Exception {
                            return v1 + v2;
                        }
                    })
                    .mapValues(
                            new Function<Double, Double>() {
                                public Double call(Double v1) throws Exception {
                                    return DUMPING_FACTOR * v1 + (1. - DUMPING_FACTOR);
                                }
                            }
                    );
        }

        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = pageRanks.mapToPair(
                new PairFunction<Tuple2<String, Double>, ImmutableBytesWritable, Put>() {
                    public Tuple2<ImmutableBytesWritable, Put>
                    call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                        Put put = new Put(Bytes.toBytes(stringDoubleTuple2._1));
                        put.addColumn(CF_COLUMN_FAMILY, PAGE_RANK, Bytes.toBytes(stringDoubleTuple2._2));

                        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
                    }
                }
        );

        hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
    }
}
