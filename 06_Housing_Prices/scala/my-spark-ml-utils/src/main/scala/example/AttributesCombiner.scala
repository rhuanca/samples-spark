package example

import org.apache.spark.sql.types._
import org.apache.spark.ml.param._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.ml.util.DefaultParamsReadable
import org.apache.spark.ml.Transformer



trait HasAddBedRooms extends Params {
  final val addBedRooms: Param[Boolean] = new Param[Boolean](this, "addBedRooms", "add bedrooms column")
  final def getAddBedRooms: Boolean = $(addBedRooms)
}

object AttributesCombiner extends DefaultParamsReadable[AttributesCombiner]


class AttributesCombiner(override val uid: String) extends Transformer with DefaultParamsWritable with HasAddBedRooms 
{
    def this() = this(Identifiable.randomUID("AttributesCombiner"))
    
    def setAddBedRooms(value: Boolean): AttributesCombiner = set(addBedRooms, value).asInstanceOf[AttributesCombiner]
    
    def transformSchema(schema: org.apache.spark.sql.types.StructType): org.apache.spark.sql.types.StructType = {
        val output_schema = DataTypes.createStructType(
            schema.fields.map( x=> {
                DataTypes.createStructField(x.name, x.dataType, x.nullable)
            }) ++
            Array(DataTypes.createStructField("rooms_per_household", DataTypes.DoubleType, false)) ++
            Array(DataTypes.createStructField("population_per_household", DataTypes.DoubleType, false)) 
        )
        
        if($(addBedRooms)) {
            output_schema.add(DataTypes.createStructField("bedrooms_per_room", DataTypes.DoubleType, false))
        } else {
            output_schema
        }
    }

    def transform(dataset: org.apache.spark.sql.Dataset[_]): org.apache.spark.sql.DataFrame = {
        val common = dataset
            .withColumn("rooms_per_household", col("total_rooms")/col("households"))
            .withColumn("population_per_household", col("population")/col("households"))
        
        if($(addBedRooms)) {
            common.withColumn("bedrooms_per_room", col("total_bedrooms_imputed")/col("total_rooms"))
        } else {
            common
        }
        
    }
    
    override def copy(extra: ParamMap): AttributesCombiner = defaultCopy(extra)

}

