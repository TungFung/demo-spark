package com.example.streaming.receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_SER_2) with Logging {

  override def onStart(): Unit = {
    new Thread(){
      override def run(): Unit = {
        receive()
      }

      def receive(): Unit = {
        var socket: Socket = null
        var userInput: String = null
        try {
          logInfo("Connecting to " + host + ":" + port)
          socket = new Socket(host, port)
          logInfo("Connected to " + host + ":" + port)

          val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
          userInput = reader.readLine()

          while(!isStopped && userInput != null) { //isStopped 是 extend Receiver的方法
            store(userInput)
            userInput = reader.readLine()
          }

          reader.close()
          socket.close()
          logInfo("Stopped receiving")
          restart("Trying to connect again") //restart 是 extend Receiver的方法
        } catch {
          case e: java.net.ConnectException => restart("Error connecting to " + host + ":" + port, e)
          case t: Throwable => restart("Error receiving data", t)
        }
      }

    }.start()
  }

  override def onStop(): Unit = {}


}
