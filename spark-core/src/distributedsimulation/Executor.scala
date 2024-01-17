package distributedsimulation

import java.io.InputStream
import java.net.{ServerSocket, Socket}


object Executor {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(9999)
    println("服务端启动了")
    val client:Socket =  server.accept()
    val input:InputStream= client.getInputStream
    val readContent:Int = input.read
    println("接受到数据"+readContent)
    input.close()
    client.close()
    server.close()
  }
}
