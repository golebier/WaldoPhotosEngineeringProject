import java.io.BufferedInputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.io.Reader
import java.net.HttpURLConnection
import java.net.URL
import org.apache.commons.imaging.common.bytesource.ByteSource
import org.apache.commons.imaging.common.bytesource.ByteSourceArray
import org.apache.commons.imaging.common.bytesource.ByteSourceInputStream
import org.apache.commons.imaging.common.IImageMetadata
import org.apache.commons.imaging.common.IImageMetadata
import org.apache.commons.imaging.common.ImageMetadata
import org.apache.commons.imaging.formats.jpeg.JpegImageMetadata
import org.apache.commons.imaging.formats.jpeg.JpegImageMetadata
import org.apache.commons.imaging.formats.jpeg.JpegImageParser
import org.apache.commons.imaging.formats.png.PngImageParser
import org.apache.commons.imaging.formats.tiff.constants.ExifTagConstants._
import org.apache.commons.imaging.formats.tiff.taginfos.TagInfo
import org.apache.commons.imaging.formats.tiff.TiffField
import org.apache.commons.imaging.formats.tiff.TiffField
import org.apache.commons.imaging.formats.tiff.TiffImageMetadata
import org.apache.commons.imaging.formats.tiff.TiffImageMetadata
import org.apache.commons.imaging.ImageInfo
import org.apache.commons.imaging.ImageReadException
import org.apache.commons.imaging.Imaging
import org.apache.commons.imaging.Imaging
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions._
import scala.io.Source
import spark.implicits._

def prepareRdd(j: JpegImageMetadata): String = {
    try {
        val exif = j.getExif();
        val json = if (exif != null) {
            // TODO prepare better conversion for json converter with proper types
            exif.getAllFields().map(x => "\""+x.getTagName+"\": \""+x.getValueDescription+"\"").mkString("{", ", ", "}")
        } else ""
        json
    } catch {
        // Log any exceptions, but return only empty string
        case e: Exception => println("Exception catched: " + e)
        case _: Throwable => println("Throwable!")
    }
    ""
}

def prepareDfExifRowFromAImage(url: String): String = {
    try {
        val u = new URL(url)
        val uc = u.openConnection()
        val in = new BufferedInputStream(uc.getInputStream())
        val bsis = new ByteSourceInputStream(in, url)
        val parser = new JpegImageParser()
        val metadata = parser.getMetadata(bsis)
        val json = if (metadata.isInstanceOf[JpegImageMetadata]) {
            val jpegMetadata = metadata.asInstanceOf[JpegImageMetadata]
            prepareRdd(jpegMetadata)
        } else ""
        json
    } catch {
        // Log any exceptions, but return only empty string
        case e: Exception => println("Exception catched: " + e)
        case _: Throwable => println("Throwable!")
    }
    ""
}
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// For url
val urlBase = "http://s3.amazonaws.com/waldo-recruiting/"
// prepare html form url
val html = Source.fromURL(urlBase)
// build xml from html
val xml = scala.xml.XML.loadString(html.mkString)
// extract all key tabs from xml
val keys = (xml \ "Contents" \ "Key").map(_.text).filter(_.endsWith(".jpg")) // TODO it should be done better, we have that info later so it can be done as matcher
// parallaze key to use it on cluster
val keysRdd = sc.parallelize(keys)
// use power of spark do execute each extraction on each worker
val exifJsons = keysRdd.filter(!_.isEmpty).map("http://s3.amazonaws.com/waldo-recruiting/"+_}).map(prepareDfExifRowFromAImage(_))
val exifsDf = sqlContext.read.json(exifJsons)
// display 30 rows from DF
//exifsDf.take(30)

// this part shows that app works fine, but for list of URLs I need to find serialization problem. ?https://forums.databricks.com/questions/369/how-do-i-handle-a-task-not-serializable-exception.html?
//val keysRdd = sc.parallelize(Array("http://s3.amazonaws.com/waldo-recruiting/01b819c4-c765-4dca-a407-609b64954126.a17c6591-de20-4b75-a5de-0bb11a34a116.jpg", "")).filter(!_.isEmpty).map(prepareDfExifRowFromAImage(_))// val jsonRdd = sc.parallelize(jsons)
//keysRdd.take(3)
//val exifsDf = sqlContext.read.json(keysRdd)
//exifsDf.take(3)

sys.exit
////val hqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
//// df.registerTempTable("tempTable") 
//// hqlContext.sql("INSERT INTO dbName.tableName SELECT * FROM tempTable") // to be able use queries on HIVE and Spark
//df.write.mode(SaveMode.AppendMode).saveAsTable("dbName.tableName") // Or just Spark

//val items = metadata.getItems
//println("")

