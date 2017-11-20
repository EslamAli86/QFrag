import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.collection.JavaConversions;
import conf.*;

import java.io.Serializable;
import java.util.ArrayList;
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
    private int numPartitions = 0;
    private SparkConfiguration config = null;
    private JavaSparkContext sc = null;

    private void init() {
        String log_level = config.getLogLevel();
        LOG.info("Setting log level to " + log_level);
        LOG.setLevel(Level.toLevel(log_level));
        sc.setLogLevel(log_level.toUpperCase());
        config.setIfUnset ("num_partitions", sc.defaultParallelism());
        config.setHadoopConfig (sc.hadoopConfiguration());
        numPartitions = config.numPartitions();

        //sc.broadcast(inputFilePath);
        Broadcast<SparkConfiguration> configBC = sc.broadcast(config);

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

    private void process() {
        // create the partitions RDD
        JavaRDD globalRDD = sc.parallelize(new ArrayList<Tuple2<Integer, String>>(numPartitions), numPartitions).cache();

        // create the computation that will be executed by each partition
        // First step computation
        Computation1Function compute1Function = new Computation1Function(inputFilePath, numPartitions);
        // Second step computation
        Computation2Function compute2Function = new Computation2Function();

        // pass the the first computation function to each partition to be executed
        JavaRDD<Tuple2<Integer, String>> step1 = globalRDD.mapPartitionsWithIndex(compute1Function,false);

        // Now flatten the results
        JavaPairRDD<Integer,String> step1Flattened = step1.flatMapToPair(tuple -> {
            ArrayList list = new ArrayList();
            list.add(new Tuple2(tuple._1(), tuple._2()));
            return list.iterator();
        });

        // Now group the messages by the Id of the destination partition
        // and execute the second computation step
        JavaRDD step2Results = step1Flattened.groupByKey().mapPartitionsWithIndex(compute2Function,false);
        // in case you want to store the results of the map function in memory
        // step2Results.persist(StorageLevel.MEMORY_ONLY());
        // Now execute the previous set of transformations
        step2Results.foreachPartition(x -> {});
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Runner(), args));
    }
}

class Computation1Function implements Function2<Integer, Iterator<Tuple2<Integer, String>>, Iterator<Tuple2<Integer, String>>> {

    private String inputPath = "Path";
    private int numPartitions = 0;

    Computation1Function(String _inputPath, int _numPartitions) {
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

class Computation2Function implements Function2<Integer, Iterator<Tuple2<Integer, Iterable<String>>>, Iterator<Integer>> {
    @Override
    public Iterator<Integer> call(Integer partitionId, Iterator<Tuple2<Integer, Iterable<String>>> v2) throws Exception {
        Tuple2<Integer, Iterable<String>> iter = v2.next();
        Iterator<String> msgs = iter._2().iterator();

        while(msgs.hasNext()) {
            String msg = msgs.next();
            System.out.println("I am partition " + partitionId + " and I received the following message \"" + msg + "\"");
        }

        ArrayList<Integer> list = new ArrayList();
        list.add(0);
        return list.iterator();
    }
}