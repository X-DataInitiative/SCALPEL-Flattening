package fr.polytechnique.cmap.cnam.utilities

/**
  * Created by burq on 16/03/17.
  */
object CollectionTool {


  /*implicit class groupableMap[T](map:List[(String, T)]) {

    def groupByKey(): Map[String, List[T]] = {
      map.groupBy(_._1).map{
        case(key, valueList) =>
          (key, valueList.map(_._2))
      }
    }
  }*/

  implicit class groupableMap[K, V](it: Iterable[(K, V)]) {

    def groupByKey: Map[K, List[V]] = {
      it.groupBy(_._1).map {
        case (key, valueList) => (key, valueList.map(_._2).toList)
      }
    }
  }

}