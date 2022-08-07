package com.pandy.spark.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {

    def main(args: Array[String]): Unit = {
        val server = new ServerSocket(9999)

        val client: Socket = server.accept()
        val in: InputStream = client.getInputStream

        val objIn = new ObjectInputStream(in)

        val task: Task = objIn.readObject().asInstanceOf[Task]
        val res: List[Int] = task.compute()

        println("计算节点的结果" + res)

        in.close()
        objIn.close()
        client.close()
        server.close()
    }

}
