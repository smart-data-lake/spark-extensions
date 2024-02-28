package org.apache.spark.sql.confluent.json

import org.apache.spark.sql.confluent.IncompatibleSchemaException
import org.apache.spark.sql.confluent.json.JsonSchemaConverter._
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, MapType, StringType, StructType}
import org.json4s.{JArray, JObject, JString, JValue}
import org.json4s.JsonAST.{JBool, JField}

object SparkToJsonSchemaConverter {
  def convert(schema: StructType): JObject = {
    val jsonSchema = toJSchemaType(schema).toJson
    JObject(JField("$schema",JString("http://json-schema.org/draft-04/schema#")) +: jsonSchema.obj)
  }
  private def toJSchemaType(catalystType: DataType): JsonSchemaEntry = {
    catalystType match {
      case ArrayType(et, containsNull) => JSchemaArray(toJSchemaType(et))
      case MapType(StringType, vt, valueContainsNull) => JSchemaMap(toJSchemaType(vt))
      case st: StructType => JSchemaObject(st.fields.map(f => (f.name, toJSchemaType(f.dataType))).toMap, st.fields.filterNot(_.nullable).toSeq.map(_.name))
      case d: DecimalType if d.scale == 0 => JSchemaBasicType("integer")
      case d: DecimalType => JSchemaBasicType("number")
      case x => SparkToJsonTypeMap.get(x).map(JSchemaBasicType)
        .getOrElse(throw new IncompatibleSchemaException(s"Unexpected type $x."))
    }
  }
}

private trait JsonSchemaEntry {
  def tpe: String
  def toJson: JObject = JObject(JField(SchemaFieldType, JString(tpe)))
}
private case class JSchemaBasicType(override val tpe: String) extends JsonSchemaEntry
private case class JSchemaArray(items: JsonSchemaEntry) extends JsonSchemaEntry {
  override val tpe: String = "array"
  override def toJson: JObject = {
    JObject(super.toJson.obj :+ JField(SchemaFieldItems, items.toJson))
  }
}
private case class JSchemaObject(properties: Map[String,JsonSchemaEntry], required: Seq[String], additionalProperties: Boolean = false) extends JsonSchemaEntry {
  override val tpe: String = "object"
  override def toJson: JObject = {
    val propertiesFields = properties.toSeq.map { case (name, tpe) => JField(name, tpe.toJson) }
    val requiredField = if (required.nonEmpty) Some(JField(SchemaFieldRequired, JArray(required.map(f => JString(f)).toList))) else None
    JObject((super.toJson.obj :+ JField(SchemaFieldProperties, JObject(propertiesFields: _*))) ++ requiredField :+ JField(SchemaFieldAdditionalProperties, JBool.apply(additionalProperties)))
  }
}
private case class JSchemaMap(valueTpe: JsonSchemaEntry) extends JsonSchemaEntry {
  override val tpe: String = "object" // we model this as type=object with 'additionalProperties' set
  override def toJson: JObject = {
    JObject(super.toJson.obj :+ JField(SchemaFieldAdditionalProperties, valueTpe.toJson))
  }
}
