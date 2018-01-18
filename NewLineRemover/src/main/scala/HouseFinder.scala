
//Scala
object HouseFinder {
  def main(args: Array[String]): Unit = {


    val a = args(0)
    val indian = a.indexOf('I')
    val canadian = a.indexOf('C')
    val indian2 = a.lastIndexOf("I")
    val canadian2 = a.lastIndexOf("C")

    println(a.substring(indian,indian2+1).size +","+ a.substring(canadian,canadian2+1).size)


  }
}
