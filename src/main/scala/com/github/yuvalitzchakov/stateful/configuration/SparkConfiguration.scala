package com.github.yuvalitzchakov.stateful.configuration

/**
  * Created by Yuval.Itzchakov on 3/12/2017.
  */
case class SparkConfiguration(sparkMasterUrl: String,
                              checkpointDirectory: String,
                              timeoutInMinutes: Int)
