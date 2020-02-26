package com.hbc

import org.slf4j.{Logger, LoggerFactory}

trait LoggerClass {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def debug(message: => String):Unit = if (logger.isDebugEnabled) logger.debug(message)
  def debug(message: => String, ex: Throwable):Unit = if (logger.isDebugEnabled) logger.debug(message, ex)

  def info(message: => String):Unit =  if(logger.isInfoEnabled) logger.info(message)
  def info(message: => String, ex: Throwable):Unit = if (logger.isInfoEnabled) logger.info(message, ex)

  def warn(message: => String):Unit =  if(logger.isWarnEnabled) logger.warn(message)
  def warn(message: => String, ex: Throwable): Unit = if(logger.isWarnEnabled) logger.warn(message, ex)

  def error(message: => String):Unit = if(logger.isErrorEnabled) logger.error(message)
  def error(message: => String, ex: Throwable):Unit = if(logger.isErrorEnabled) logger.error(message, ex)
}
