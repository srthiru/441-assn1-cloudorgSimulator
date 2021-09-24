package HelperUtils

import com.typesafe.config.{Config, ConfigFactory}

class GetConfig

object GetConfig:
  
  def apply(confFile: String): Config = ConfigFactory.load(confFile)
