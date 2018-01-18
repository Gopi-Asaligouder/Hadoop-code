import scala.collection.mutable

/*
*Scala
* */
object Main {
  def main(args: Array[String]): Unit = {

    val a = args(0).split(",")
    val b = args(1).split(",")

    val version1Map = mapValues(a)
    val version2Map = mapValues(b)


     val finalList = updateList(version1Map,version2Map) ++ updateList(version2Map,version1Map)

      if (finalList.isEmpty) {

        println("equal")
      } else {
        println(finalList.distinct.sorted.mkString(","))
      }


  }

  def mapValues( inputMap:Array[String]):Map[String, Int]={
    val version1Map:mutable.Map[String,Int] = mutable.Map[String, Int]()
    for (key <- inputMap) {
      if (version1Map.contains(key)) {
        version1Map.put(key, version1Map.get(key).get + 1)
      } else {
        version1Map.put(key, 0)
      }

    }
    version1Map.toMap
  }


  def updateList(version1Map:Map[String,Int],version2Map:Map[String,Int]):List[String] ={
    var finalList = mutable.ListBuffer[String]()
    for (key <- version1Map.keySet) {

      if (!version2Map.contains(key)) {
        finalList = finalList :+ key
      } else {
        if (version1Map.get(key).get != version2Map.get(key).get) {
          finalList = finalList :+ key
        }

      }
    }
    finalList.toList
  }


}
