import java.io.BufferedInputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.io.Reader
import java.net.HttpURLConnection
import java.net.URL
import org.apache.commons.imaging.Imaging
import org.apache.commons.imaging.formats.jpeg.JpegImageMetadata
import org.apache.commons.imaging.formats.tiff.constants.ExifTagConstants._
import org.apache.commons.imaging.formats.tiff.TiffField
import scala.io.Source
import org.apache.spark.sql.hive.HiveContext

// prepare DataFrame Row with all Exif tags from Metadata
def prepareExifTags(jpegMetadata: JpegImageMetadata, tagInfo: TagInfo): Row[TagInfo] = {
    val field = jpegMetadata.findEXIFValueWithExactMatch(tagInfo)
    field match {
        case g: TiffField => // preapre ExifTagConstants as a Row for Df // I did not finished the function
        case _ => {}
    }
}
// prepare DataFrame Row with all Exif tags from image fetched from webside
def prepareDfExifRowFromAImage(url: String): Row[TagInfo] = {
//    val url = "http://s3.amazonaws.com/waldo-recruiting/01b819c4-c765-4dca-a407-609b64954126.a17c6591-de20-4b75-a5de-0bb11a34a116.jpg"
    val u = new URL(url)
    val uc = u.openConnection()
    val in = new BufferedInputStream(uc.getInputStream())
    val metadata = Imaging.getMetadata(in, url)
    metadata match {
        case j: JpegImageMetadata => dump(j)
    }
}

// For url
val urlBase = "http://s3.amazonaws.com/waldo-recruiting/"
// prepare html from url
val html = Source.fromURL(urlBase)
// build xml from html
val xml = scala.xml.XML.loadString(html.mkString)
// extract all key tabs from xml
val keys = (xml \ "Contents" \ "Key").map(_.text).filter(_.endsWith(".jpg")) // TODO it should be done better, we have that info later so it can be done as matcher
// parallelize key to use it on cluster
val keysRdd = sc.parallelize(keys)
// use power of spark to execute each extraction on each worker
val df = keysRdd.map(x => prepareDfExifRowFromAImage(urlBase+x)).toDF

//val hqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
// df.registerTempTable("tempTable") 
// hqlContext.sql("INSERT INTO dbName.tableName SELECT * FROM tempTable") // to be able use queries on HIVE and Spark
df.write.mode(SaveMode.AppendMode).saveAsTable("dbName.tableName") // Or just Spark

exit
