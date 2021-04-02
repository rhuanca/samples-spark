package example
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.ml.util.DefaultParamsReadable

object Hello {
    def main(args: Array[String]) = {
        val t = new AttributesCombiner().setAddBedRooms(true)
    }
}
