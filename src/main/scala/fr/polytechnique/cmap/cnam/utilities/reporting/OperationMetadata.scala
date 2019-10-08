// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.utilities.reporting

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.collection.{JavaConversions, mutable}

case class OperationMetadata(
  outputTable: String,
  outputPath: String,
  outputType: String,
  sources: List[String] = List.empty[String],
  joinKeys: List[String] = List.empty[String]) extends JsonSerializable

object OperationMetadata {

  // REASON: https://stackoverflow.com/a/22375260/795574
  def deserialize(file: String): mutable.Map[String, OperationMetadata] = {
    val ois = new ObjectInputStream(new FileInputStream(file)) {
      override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
        try {
          Class.forName(desc.getName, false, getClass.getClassLoader)
        }
        catch {
          case ex: ClassNotFoundException => super.resolveClass(desc)
        }
      }
    }
    val meta: java.util.Map[String, OperationMetadata] = ois.readObject.asInstanceOf[java.util.Map[String, OperationMetadata]]
    ois.close()
    JavaConversions.mapAsScalaMap[String, OperationMetadata](meta)
  }

  // Scala JavaConversions has issues with serialization
  private def scalaToJavaHashMapConverter(scalaMap: Map[String, OperationMetadata]): java.util.HashMap[String, OperationMetadata] = {
    val javaHashMap = new java.util.HashMap[String, OperationMetadata]()
    scalaMap.foreach(entry => javaHashMap.put(entry._1, entry._2))
    javaHashMap
  }

  def serialize(file: String, meta: Map[String, OperationMetadata]): Unit = {
    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(scalaToJavaHashMapConverter(meta))
    oos.close()
  }
}