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

        this.process2();
//        JavaRDD step2 = step1.groupBy(t -> t._1());

        return 0;
    }

    public void process1() {
        globalRDD = sc.parallelize(new ArrayList<Tuple2<Integer, String>>(), numPartitions).cache();

        JavaRDD<Tuple2<Integer, String>> step1 = globalRDD.mapPartitionsWithIndex((pId, y) -> {
            System.out.println("I am partition " + pId + "and the input files are ");//) + configBC.value());

            ArrayList<Tuple2<Integer, String>> list = new ArrayList();

            for(int i = 0 ; i < numPartitions ; ++i) {
                String msg = "This is from partition " + pId + "to partition " + i;
                list.add(new Tuple2<>(i,msg));
            }

            return list.iterator();
        },false);

        step1.persist(StorageLevel.MEMORY_ONLY());
        step1.foreachPartition(x -> {});
    }

    public void process2() {
        globalRDD = sc.parallelize(new ArrayList<Tuple2<Integer, String>>(), numPartitions).cache();

        ComputationFunction computeFunction = new ComputationFunction();

        JavaRDD<Tuple2<Integer, String>> step1 = globalRDD.mapPartitionsWithIndex(computeFunction,false);

        step1.persist(StorageLevel.MEMORY_ONLY());
        step1.foreachPartition(x -> {});

        System.out.println("Results count = " + step1.count());

        step1.foreach(x -> {
            System.out.println(x._1() + " $$$ " + x._2());
        });

/*        JavaPairRDD flatStep1 = step1.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,String>, Object, Object>() {

            @Override
            public Iterator<Tuple2<Object, Object>> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                ArrayList list = new ArrayList();
                list.add(new Tuple2<Object,Object>(integerStringTuple2._1(),integerStringTuple2._2()));
                return list.iterator();
            }
        });*/

        JavaPairRDD<Integer,String> flatStep1 = step1.flatMapToPair(tuple -> {
                ArrayList list = new ArrayList();
                list.add(new Tuple2<Integer,String>(tuple._1(), tuple._2()));
                return list.iterator();
        });

        List<Tuple2<Integer, Iterable<String>>> msgList = flatStep1.groupByKey().collect();

        for(int i = 0 ; i < msgList.size() ; ++i) {
            Iterator<String> iter = msgList.get(i)._2().iterator();
            System.out.println("I am partition " + msgList.get(i)._1() + " and I got msgs:");
            while(iter.hasNext()) {
                System.out.println(iter.next());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Runner(), args));
    }
}



class ComputationFunction implements Function2<Integer, Iterator<Step1Engine>, Iterator<Step1Engine>>, Serializable {

    public ComputationFunction() {

    }

    @Override
    public Iterator<Step1Engine> call(Integer partitionId, Iterator<Step1Engine> v2) throws Exception {

        Step1Engine engine = new Step1Engine(partitionId,8, "File");//, this.superstep, inBC);

        Iterator<Step1Engine> iter = engine.compute();

        return iter;
    }
}

class Step1Engine implements Serializable {

    int pId = 0;
    int numPartitions = 0;
    String inputFile;

    public Step1Engine(int _pId, int _numPartitions, String _inputFile) {
        pId = _pId;
        numPartitions = _numPartitions;
        inputFile = _inputFile;
    }

    public Iterator compute() {
        System.out.println("I am partition " + pId + " and the input files are " + inputFile);//) + configBC.value());

        ArrayList<Tuple2<Integer, String>> list = new ArrayList();

        for(int i = 0 ; i < numPartitions ; ++i) {
            String msg = "This is from partition " + pId + " to partition " + i;
            list.add(new Tuple2<>(i,msg));
        }

        return list.iterator();
    }
}
