package com.pandy.spark.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.{ServerSocket, Socket}

object Driver {

    def main(args: Array[String]): Unit = {

        var client = new Socket("localhost", 9999)

        val out: OutputStream = client.getOutputStream
        val objOut = new ObjectOutputStream(out)

        val task = new Task()
        objOut.writeObject(task)

        out.write(2)

        out.flush()
        out.close()

        client.close()
        println("客户端数据发送完毕")
    }

}
