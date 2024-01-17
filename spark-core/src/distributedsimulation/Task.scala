package distributedsimulation

class Task {
  val datas = List(1,2,3,4)
  val logic = (num:Int)=>{num*2}
  def compute():List[Int] = {
    datas.map(logic)
  }
}
