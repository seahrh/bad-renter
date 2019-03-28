package com.sgcharts.badrenter

import org.apache.log4j.Logger

trait Log4jLogging {
  @transient protected lazy val log: Logger = Logger.getLogger(getClass.getCanonicalName)

}
