import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Add {
  val rows = 100
  val columns = 100

  case class Block ( data: Array[Double] ) {
   override
    def toString (): String = {
      var s = "\n"
      for ( i <- 0 until rows ) {
        for ( j <- 0 until columns )
          s += "\t%.3f".format(data(i*rows+j))
        s += "\n"
      }
      s
    }
  }
  def toBlock ( triples: List[(Int,Int,Double)] ): Block = {
 var blockArray:Array[Double] = new Array[Double](rows*columns)
    triples.foreach(t=> {
      blockArray(t._1*rows+t._2) = t._3
    })

    return Block(blockArray)

  }
  def blockAdd ( m: Block, n: Block ): Block = {
 var block_add:Array[Double] = new Array[Double](rows*columns)
    for ( i <- 0 until 100 ) {
      for ( j <- 0 until 100 )
        block_add(i*rows+j) = m.data(i*rows+j) + n.data(i*rows+j)
    }
    return Block(block_add)

  }
  /* Read a sparse matrix from a file and convert it to a block matrix */
  def createBlockMatrix ( sc: SparkContext, file: String ): RDD[((Int,Int),Block)] = {
  val f = sc.textFile(file).map( line => { val a = line.split(",")
      ((a(0).toInt/rows, a(1).toInt/columns), (a(0).toInt%rows, a(1).toInt%columns, a(2).toDouble)) } )
      .groupByKey().map{ case (key,value) => (key, toBlock(value.toList)) }
      return f
  }

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Add")
    val sc = new SparkContext(conf)
    val M = createBlockMatrix(sc,args(0))
    val N =  createBlockMatrix(sc,args(1))
    val output = M.join(N).map{ case(key, (m, n)) => (key, blockAdd(m, n)) }
    output.saveAsTextFile(args(4))
    sc.stop()
  
  }
}
