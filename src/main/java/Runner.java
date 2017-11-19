import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.collection.JavaConversions;
import conf.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import scala.Tuple2;

/**
 * Created by ehussein on 11/16/17.
 */
public class Runner implements Tool {
    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(Runner.class);
    /**
     * Writable conf
     */
    private Configuration conf;

    private String inputFilePath = null;
    private Broadcast<String> inputBC = null;
    private Broadcast<SparkConfiguration> configBC;
    private int numPartitions = 0;
    JavaRDD globalRDD = null;
    SparkConfiguration config = null;
    JavaSparkContext sc = null;

    public void init() {
        String log_level = config.getLogLevel();
        LOG.info("Setting log level to " + log_level);
        LOG.setLevel(Level.toLevel(log_level));
        sc.setLogLevel(log_level.toUpperCase());
        config.setIfUnset ("num_partitions", sc.defaultParallelism());
        config.setHadoopConfig (sc.hadoopConfiguration());
        numPartitions = config.numPartitions();

        inputBC = sc.broadcast(inputFilePath);
        configBC = sc.broadcast(config);

        configBC.value().initialize();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        YamlConfiguration yamlConfig = new YamlConfiguration(args);
        config = new SparkConfiguration (JavaConversions.mapAsScalaMap(yamlConfig.getProperties()));
        sc = new JavaSparkContext(config.sparkConf());

        inputFilePath = "hdfs:///input/params.txt";

        init();

        process();

        return 0;
    }

    public void process() {
        // create the partitions RDD
        globalRDD = sc.parallelize(new ArrayList<Tuple2<Integer, String>>(), numPartitions).cache();

        // create the computation that will be executed by each partition
        ComputationFunction computeFunction = new ComputationFunction(inputFilePath, numPartitions);

        // pass the computation function to each partition to be executed
        JavaRDD<Tuple2<Integer, String>> step1 = globalRDD.mapPartitionsWithIndex(computeFunction,false);

        // store the results of the map function in memory
        step1.persist(StorageLevel.MEMORY_ONLY());
        // materialize the map function (i.e. execute the computation function)
        step1.foreachPartition(x -> {});

        // Now flatten the results
        JavaPairRDD<Integer,String> step1Flattened = step1.flatMapToPair(tuple -> {
            ArrayList list = new ArrayList();
            list.add(new Tuple2(tuple._1(), tuple._2()));
            return list.iterator();
        });

        // Now group the messages by the Id of the destination partition
        // and print the messages that the destination partition
        // received from other partitions
        step1Flattened.groupByKey().foreach( group -> {
            // print the Id of the destination partition
            System.out.println("I am partition " + group._1() + ", and I received the following messages:");
            Iterator<String> iter = group._2().iterator();
            while(iter.hasNext()) {
                System.out.println(iter.next());
            }
        });

        /*List<Tuple2<Integer, Iterable<String>>> msgList = flatStep1.groupByKey().collect();

        for(int i = 0 ; i < msgList.size() ; ++i) {
            Iterator<String> iter = msgList.get(i)._2().iterator();
            System.out.println("I am partition " + msgList.get(i)._1() + " and I got the following messages:");
            while(iter.hasNext()) {
                System.out.println(iter.next());
            }
        }*/
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Runner(), args));
    }
}

class ComputationFunction implements Function2<Integer, Iterator<Tuple2<Integer, String>>, Iterator<Tuple2<Integer, String>>>, Serializable {

    String inputPath = "Path";
    int numPartitions = 0;

    public ComputationFunction(String _inputPath, int _numPartitions) {
        this.inputPath = _inputPath;
        this.numPartitions = _numPartitions;
    }

    @Override
    public Iterator<Tuple2<Integer, String>> call(Integer partitionId, Iterator<Tuple2<Integer, String>> v2) throws Exception {
        // Who am I?
        System.out.println("I am partition " + partitionId + " and the input files are " + inputPath);

        ArrayList<Tuple2<Integer, String>> list = new ArrayList();

        // Send my message to the other partitions
        for(int i = 0 ; i < numPartitions ; ++i) {
            String msg = "This is from partition " + partitionId + " to partition " + i;
            list.add(new Tuple2<>(i,msg));
        }

        return list.iterator();
    }
}
