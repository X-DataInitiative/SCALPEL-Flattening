package fr.polytechnique.cmap.cnam.utilities

/**
  * Created by burq on 16/03/17.
  */
object CollectionTool {

  implicit class groupableMap[K, V](it: Iterable[(K, V)]) {

    def groupByKey: Map[K, List[V]] = {
      it.groupBy(_._1).mapValues(_.map(_._2).toList)
    }
  }

}
