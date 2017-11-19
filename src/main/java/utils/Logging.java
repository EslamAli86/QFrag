package utils;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ehussein on 10/16/17.
 */
public class Logging {
    protected String logName = this.getClass().getSimpleName();

    protected Logger log = LoggerFactory.getLogger (logName);

    /** client functions are called by name in order to avoid unecessary string
     *  building **/

    protected void logInfo(String msg){
        if (log.isInfoEnabled())
            log.info (msg);
    }

    protected void logWarning(String msg) {
        if (log.isWarnEnabled())
            log.warn (msg);
    }

    protected void logDebug(String msg) {
        if (log.isDebugEnabled())
            log.debug (msg);
    }

    protected void logError(String msg) {
        if (log.isErrorEnabled())
            log.error (msg);
    }

    protected void setLogLevel(String level) {
        LogManager.getLogger(logName).setLevel(Level.toLevel(level));
    }
}
