/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.Queue

  object QueueStream {

    def main(args: Array[String]) {
      val sparkConf = new SparkConf().setAppName("MyCount").setMaster("local[*]")
      // Create the context
      val ssc = new StreamingContext(sparkConf, Seconds(1))

      // Create the queue through which RDDs can be pushed to
      // a QueueInputDStream
      val rddQueue: mutable.Queue[RDD[Int]] = new Queue[RDD[Int]]()

      val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue)
      val mappedStream: DStream[(Int, Int)] = inputStream.map(x => (x % 10, 1))
      val reducedStream = mappedStream.reduceByKey(_ + _)
      reducedStream.print()
      ssc.start()

      // Create and push some RDDs into rddQueue
      for (i <- 1 to 30) {
        rddQueue.synchronized {
          rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
        }
        Thread.sleep(1000)
      }
      ssc.stop()
    }
  }

