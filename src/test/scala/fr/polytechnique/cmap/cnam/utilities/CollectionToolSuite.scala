package fr.polytechnique.cmap.cnam.utilities

import org.scalatest.FlatSpecLike

class CollectionToolSuite extends FlatSpecLike {

  "groupByKey" should "group a list of pair by key and create a list with the rest" in {
    // Given
    val inputList: List[(String, String)] = List(
      ("key1", "value1"),
      ("key1", "value2"),
      ("key2", "value2"),
      ("key2", "value3")
    )

    val expected: Map[String, List[String]] = Map(
      "key1" -> List("value1", "value2"),
      "key2" -> List("value2", "value3")
    )

    // When
    import fr.polytechnique.cmap.cnam.utilities.CollectionTool._
    val result = inputList.groupByKey()

    // Then
    assert(result == expected)
  }

}
