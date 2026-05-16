package io.github.sparkasn1.spark.asn1.util

import org.apache.hadoop.conf.Configuration

/** Hadoop Configuration wrapper safe to serialize into a Spark closure or broadcast. */
class SerializableHadoopConf(@transient private var conf: Configuration)
  extends Serializable {

  def get: Configuration = conf

  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    conf.write(out)
  }
  private def readObject(in: java.io.ObjectInputStream): Unit = {
    conf = new Configuration(false)
    conf.readFields(in)
  }
}
