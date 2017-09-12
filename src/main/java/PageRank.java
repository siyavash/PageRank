import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PageRank {
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

    }
}
