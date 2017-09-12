import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class PageRank {

    private static final int NUM_OF_PAGE_RANK_ITERATES = 42;

    private static final byte[] SL_COLUMN_FAMILY = "sl".getBytes();
    private static final byte[] SUB_LINKS_COLUMN = "subLinks".getBytes();

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("PageRank").setMaster("spark://master:7077");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        Configuration hbaseConfiguration = HBaseConfiguration.create();

        hbaseConfiguration.set("hbase.master", "master:60000");
        hbaseConfiguration.set("zookeeper.znode.parent", "/hbase");
        hbaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConfiguration.set("hbase.zookeeper.quorum", "master,slave");
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, "wb");

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
                            return null;
                        }
                    }
            );
        }

    }
}
