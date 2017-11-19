package conf;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import utils.Logging;

import java.util.*;

public class Configuration extends Logging implements java.io.Serializable {
    
    // temp solution to cope with static configurations: changing soon
    protected UUID uuid = UUID.randomUUID();

    private static final Logger LOG = Logger.getLogger(Configuration.class);
    public static final String CONF_LOG_LEVEL = "arabesque.log.level";
    public static final String CONF_LOG_LEVEL_DEFAULT = "info";
    public static final String CONF_MAINGRAPH_CLASS = "arabesque.graph.class";
    public static final String CONF_MAINGRAPH_CLASS_DEFAULT = "io.arabesque.graph.BasicMainGraph";
    public static final String CONF_MAINGRAPH_PATH = "arabesque.graph.location";
    public static final String CONF_MAINGRAPH_PATH_DEFAULT = "main.graph";
    public static final String CONF_MAINGRAPH_LOCAL = "arabesque.graph.local";
    public static final boolean CONF_MAINGRAPH_LOCAL_DEFAULT = false;

    public static final String CONF_OUTPUT_ACTIVE = "arabesque.output.active";
    public static final boolean CONF_OUTPUT_ACTIVE_DEFAULT = true;

    public static final String INFO_PERIOD = "arabesque.info.period";
    public static final long INFO_PERIOD_DEFAULT = 60000;

    public static final String CONF_COMM_STRATEGY = "arabesque.comm.strategy";
    public static final String CONF_COMM_STRATEGY_DEFAULT = "odag_sp";

    public static final String CONF_COMM_STRATEGY_ODAGMP_MAX = "arabesque.comm.strategy.odagmp.max";
    public static final int CONF_COMM_STRATEGY_ODAGMP_MAX_DEFAULT = 100;

    public static final String CONF_COMM_STRATEGY_FACTORY_CLASS = "arabesque.comm.factory.class";

    public static final String CONF_EZIP_AGGREGATORS = "arabesque.odag.aggregators";
    public static final int CONF_EZIP_AGGREGATORS_DEFAULT = -1;

    public static final String CONF_ODAG_FLUSH_METHOD = "arabesque.odag.flush.method";
    public static final String CONF_ODAG_FLUSH_METHOD_DEFAULT = "flush_by_parts";

    private static final String CONF_2LEVELAGG_ENABLED = "arabesque.2levelagg.enabled";
    private static final boolean CONF_2LEVELAGG_ENABLED_DEFAULT = true;
    private static final String CONF_FORCE_GC = "arabesque.forcegc";
    private static final boolean CONF_FORCE_GC_DEFAULT = false;

    public static final String CONF_OUTPUT_PATH = "arabesque.output.path";
    public static final String CONF_OUTPUT_PATH_DEFAULT = "Output";

    public static final String CONF_DEFAULT_AGGREGATOR_SPLITS = "arabesque.aggregators.default_splits";
    public static final int CONF_DEFAULT_AGGREGATOR_SPLITS_DEFAULT = 1;

    public static final String CONF_INCREMENTAL_AGGREGATION = "arabesque.aggregation.incremental";
    public static final boolean CONF_INCREMENTAL_AGGREGATION_DEFAULT = false;

    public static final String CONF_AGGREGATION_STORAGE_CLASS = "arabesque.aggregation.storage.class";
    public static final String CONF_AGGREGATION_STORAGE_CLASS_DEFAULT = "io.arabesque.aggregation.AggregationStorage";

    protected static Configuration instance = null;
    private ImmutableClassesGiraphConfiguration giraphConfiguration;

    private boolean useCompressedCaches;
    private int cacheThresholdSize;
    private long infoPeriod;
    private int odagNumAggregators;
    private boolean is2LevelAggregationEnabled;
    private boolean forceGC;


    private String outputPath;
    private int defaultAggregatorSplits;

    private boolean isGraphEdgeLabelled;
    protected boolean initialized = false;
    private boolean isGraphMulti;

    public UUID getUUID() {
       return uuid;
    }

    public static boolean isUnset() {
       return instance == null;
    }

    public static <C extends Configuration> C get() {
        if (instance == null) {
           LOG.error ("instance is null");
            throw new RuntimeException("Oh-oh, Null configuration");
        }

        if (!instance.isInitialized()) {
           instance.initialize();
        }

        return (C) instance;
    }

    public static void unset() {
       instance = null;
    }

    public synchronized static void setIfUnset(Configuration configuration) {
        if (isUnset()) {
            set(configuration);
        }
    }

    public static void set(Configuration configuration) {
        instance = configuration;

        // Whenever we set configuration, reset all known pools
        // Since they might have initialized things based on a previous configuration
        // NOTE: This is essential for the unit tests
/*        for (Pool pool : PoolRegistry.instance().getPools()) {
            pool.reset();
        }*/
    }

    public Configuration(ImmutableClassesGiraphConfiguration giraphConfiguration) {
        this.giraphConfiguration = giraphConfiguration;
    }

    public Configuration() {}

    public void initialize() {
        if (initialized) {
            return;
        }

        LOG.info("Initializing Configuration...");

        infoPeriod = getLong(INFO_PERIOD, INFO_PERIOD_DEFAULT);
        odagNumAggregators = getInteger(CONF_EZIP_AGGREGATORS, CONF_EZIP_AGGREGATORS_DEFAULT);
        is2LevelAggregationEnabled = getBoolean(CONF_2LEVELAGG_ENABLED, CONF_2LEVELAGG_ENABLED_DEFAULT);
        forceGC = getBoolean(CONF_FORCE_GC, CONF_FORCE_GC_DEFAULT);

        //outputPath = getString(CONF_OUTPUT_PATH, CONF_OUTPUT_PATH_DEFAULT + "_" + computationClass.getName());

        defaultAggregatorSplits = getInteger(CONF_DEFAULT_AGGREGATOR_SPLITS, CONF_DEFAULT_AGGREGATOR_SPLITS_DEFAULT);

/*        Computation<?> computation = createComputation();
        computation.initAggregations();*/

        initialized = true;
        LOG.info("Configuration initialized");
    }

    public boolean isInitialized() {
       return initialized;
    }

    public ImmutableClassesGiraphConfiguration getUnderlyingConfiguration() {
        return giraphConfiguration;
    }

    public String getString(String key, String defaultValue) {
        return giraphConfiguration.get(key, defaultValue);
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        return giraphConfiguration.getBoolean(key, defaultValue);
    }

    public Integer getInteger(String key, Integer defaultValue) {
        return giraphConfiguration.getInt(key, defaultValue);
    }

    public Long getLong(String key, Long defaultValue) {
        return giraphConfiguration.getLong(key, defaultValue);
    }

    public Float getFloat(String key, Float defaultValue) {
        return giraphConfiguration.getFloat(key, defaultValue);
    }

    public Class<?> getClass(String key, String defaultValue) {
        try {
            return Class.forName(getString(key, defaultValue));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public String[] getStrings(String key, String... defaultValues) {
        return giraphConfiguration.getStrings(key, defaultValues);
    }

    public Class<?>[] getClasses(String key, Class<?>... defaultValues) {
        return giraphConfiguration.getClasses(key, defaultValues);
    }

    public boolean isUseCompressedCaches() {
        return useCompressedCaches;
    }

    public int getCacheThresholdSize() {
        return cacheThresholdSize;
    }

    public String getLogLevel() {
       return getString (CONF_LOG_LEVEL, CONF_LOG_LEVEL_DEFAULT);
    }

    public String getMainGraphPath() {
        return getString(CONF_MAINGRAPH_PATH, CONF_MAINGRAPH_PATH_DEFAULT);
    }

    public long getInfoPeriod() {
        return infoPeriod;
    }



    public boolean isOutputActive() {
        return getBoolean(CONF_OUTPUT_ACTIVE, CONF_OUTPUT_ACTIVE_DEFAULT);
    }

    public int getODAGNumAggregators() {
        return odagNumAggregators;
    }

    public String getOdagFlushMethod() {
       return getString(CONF_ODAG_FLUSH_METHOD, CONF_ODAG_FLUSH_METHOD_DEFAULT);
    }

    public int getMaxEnumerationsPerMicroStep() {
        return 10000000;
    }

    public boolean is2LevelAggregationEnabled() {
        return is2LevelAggregationEnabled;
    }

    public boolean isForceGC() {
        return forceGC;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public boolean isGraphEdgeLabelled() {
        return isGraphEdgeLabelled;
    }

    public boolean isGraphMulti() {
        return isGraphMulti;
    }

    public boolean isAggregationIncremental() {
       return getBoolean (CONF_INCREMENTAL_AGGREGATION, CONF_INCREMENTAL_AGGREGATION_DEFAULT);
    }

    public int getMaxOdags() {
       return getInteger (CONF_COMM_STRATEGY_ODAGMP_MAX, CONF_COMM_STRATEGY_ODAGMP_MAX_DEFAULT);
    }

    public String getCommStrategy() {
       return getString (CONF_COMM_STRATEGY, CONF_COMM_STRATEGY_DEFAULT);
    }

}

