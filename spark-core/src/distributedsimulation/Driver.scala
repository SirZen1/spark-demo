package distributedsimulation

import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    val client = new Socket("localhost", 9999)
    val out = client.getOutputStream
    out.write(3)
    out.flush()
    out.close()
    client.close()
  }
}
