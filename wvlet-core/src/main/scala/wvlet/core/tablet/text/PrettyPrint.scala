package wvlet.core.tablet.text

import wvlet.core.tablet.msgpack.MessageFormatter
import wvlet.core.tablet.obj.ObjectTabletReader
import wvlet.log.LogSupport
import wvlet.obj.ObjectSchema

/**
  *
  */
object PrettyPrint extends LogSupport {

  private val defaultPrinter = new PrettyPrint()

  def show[A](seq: Seq[A], limit: Int = 20) {
    defaultPrinter.pp(seq.take(limit))
  }

  def pp[A](seq: Seq[A]) {
    info(defaultPrinter.pf(seq).mkString("\n"))
  }

  def screenTextLength(s: String): Int = {
    (for (i <- 0 until s.length) yield {
      val ch = s.charAt(i)
      if (ch < 128) {
        1
      }
      else {
        2
      }
    }).sum
  }

  def maxColWidths(rows: Seq[Seq[String]], max:Int): IndexedSeq[Int] = {
    if(rows.isEmpty) {
      IndexedSeq(0)
    }
    else {
      val maxWidth = (0 until rows.head.length).map(i => 0).toArray
      for (r <- rows; (c, i) <- r.zipWithIndex) {
        maxWidth(i) = math.min(max, math.max(screenTextLength(c), maxWidth(i)))
      }
      maxWidth.toIndexedSeq
    }
  }

  def pad(s: String, colWidth: Int): String = {
    val str = (" " * Math.max(0, colWidth - screenTextLength(s))) + s
    if(str.length >= colWidth) {
      str.substring(0, colWidth)
    }
    else {
      str
    }
  }

}

class PrettyPrint(codec: Map[Class[_], MessageFormatter[_]] = Map.empty, maxColWidth:Int = 100) extends LogSupport {
  def show[A](seq: Seq[A], limit: Int = 20) {
    pp(seq.take(limit))
  }

  def pp[A](seq: Seq[A]) {
    println(pf(seq).mkString("\n"))
  }

  def pf[A](seq: Seq[A]): Seq[String] = {
    val b = Seq.newBuilder[Seq[String]]
    val paramNames = seq.headOption.map {x =>
      val schema = ObjectSchema(x.getClass)
      b += schema.parameters.map(_.name).toSeq
    }

    val reader = new ObjectTabletReader(seq, codec)
    b ++= (reader | RecordPrinter)
    val s = Seq.newBuilder[String]
    val rows = b.result
    val colWidth = PrettyPrint.maxColWidths(rows, maxColWidth)
    for (r <- rows) {
      val cols = for ((c, i) <- r.zipWithIndex) yield PrettyPrint.pad(c, colWidth(i))
      s += cols.mkString(" ")
    }
    s.result()
  }

}

