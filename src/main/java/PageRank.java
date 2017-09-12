import org.apache.spark.SparkConf;

public class PageRank {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("PageRank").setMaster("spark://master:7077");
    }
}
