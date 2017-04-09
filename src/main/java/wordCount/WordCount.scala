package wordCount

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sghipr on 4/9/17.
 */
object WordCount {

  def main (args: Array[String]){

    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val input = "/user/sghipr/input/README.md" //注意,这个远程地址.
    val outputFile = "/user/sghipr/wordCount"
    val lines = sc.textFile(input)
    val data = lines.flatMap(line => line.split(" "))
    val data2 = data
      .map(t => (t,1))
      .reduceByKey((x,y)=> x+y)

    // if exists,then delete
    val hadoopConf = new Configuration()
    FileSystem.get(hadoopConf).delete(new Path(outputFile),true)

    data2.saveAsTextFile(outputFile)
    printf("Successfully!\n")
    sc.stop()
  }

}
