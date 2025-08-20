package org.apache.spark.sql.custom

import ch.zzeekk.spark.expressions.ExpressionEvaluatorFactory
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedAggregator, UserDefinedFunction}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag
import scala.reflect.runtime.universe


object SparkExpressionEvaluatorFactory extends ExpressionEvaluatorFactory {

  override def getEvaluator[T <: Product : universe.TypeTag, R: universe.TypeTag : ClassTag](expression: String): ExpressionEvaluator[T, R] = {
    new ExpressionEvaluator[T, R](parseExpression(expression))
  }

  override def registerUdf[RT: universe.TypeTag](name: String, f: () => RT): Unit = ExpressionEvaluator.registerUdf(name, exprs => applyUdf(udf(f), exprs))

  override def registerUdf[RT: universe.TypeTag, A1: universe.TypeTag](name: String, f: A1 => RT): Unit = ExpressionEvaluator.registerUdf(name, exprs => applyUdf(udf(f), exprs))

  override def registerUdf[RT: universe.TypeTag, A1: universe.TypeTag, A2: universe.TypeTag](name: String, f: (A1, A2) => RT): Unit = ExpressionEvaluator.registerUdf(name, exprs => applyUdf(udf(f), exprs))

  override def registerUdf[RT: universe.TypeTag, A1: universe.TypeTag, A2: universe.TypeTag, A3: universe.TypeTag](name: String, f: (A1, A2, A3) => RT): Unit = ExpressionEvaluator.registerUdf(name, exprs => applyUdf(udf(f), exprs))

  override def registerUdf[RT: universe.TypeTag, A1: universe.TypeTag, A2: universe.TypeTag, A3: universe.TypeTag, A4: universe.TypeTag](name: String, f: (A1, A2, A3, A4) => RT): Unit = ExpressionEvaluator.registerUdf(name, exprs => applyUdf(udf(f), exprs))

  override def registerUdf[RT: universe.TypeTag, A1: universe.TypeTag, A2: universe.TypeTag, A3: universe.TypeTag, A4: universe.TypeTag, A5: universe.TypeTag](name: String, f: (A1, A2, A3, A4, A5) => RT): Unit = ExpressionEvaluator.registerUdf(name, exprs => applyUdf(udf(f), exprs))

  override def registerUdf[RT: universe.TypeTag, A1: universe.TypeTag, A2: universe.TypeTag, A3: universe.TypeTag, A4: universe.TypeTag, A5: universe.TypeTag, A6: universe.TypeTag](name: String, f: (A1, A2, A3, A4, A5, A6) => RT): Unit = ExpressionEvaluator.registerUdf(name, exprs => applyUdf(udf(f), exprs))

  override def registerUdf[RT: universe.TypeTag, A1: universe.TypeTag, A2: universe.TypeTag, A3: universe.TypeTag, A4: universe.TypeTag, A5: universe.TypeTag, A6: universe.TypeTag, A7: universe.TypeTag](name: String, f: (A1, A2, A3, A4, A5, A6, A7) => RT): Unit = ExpressionEvaluator.registerUdf(name, exprs => applyUdf(udf(f), exprs))

  override def registerUdf[RT: universe.TypeTag, A1: universe.TypeTag, A2: universe.TypeTag, A3: universe.TypeTag, A4: universe.TypeTag, A5: universe.TypeTag, A6: universe.TypeTag, A7: universe.TypeTag, A8: universe.TypeTag](name: String, f: (A1, A2, A3, A4, A5, A6, A7, A8) => RT): Unit = ExpressionEvaluator.registerUdf(name, exprs => applyUdf(udf(f), exprs))

  override def registerUdf[RT: universe.TypeTag, A1: universe.TypeTag, A2: universe.TypeTag, A3: universe.TypeTag, A4: universe.TypeTag, A5: universe.TypeTag, A6: universe.TypeTag, A7: universe.TypeTag, A8: universe.TypeTag, A9: universe.TypeTag](name: String, f: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => RT): Unit = ExpressionEvaluator.registerUdf(name, exprs => applyUdf(udf(f), exprs))

  override def registerUdf[RT: universe.TypeTag, A1: universe.TypeTag, A2: universe.TypeTag, A3: universe.TypeTag, A4: universe.TypeTag, A5: universe.TypeTag, A6: universe.TypeTag, A7: universe.TypeTag, A8: universe.TypeTag, A9: universe.TypeTag, A10: universe.TypeTag](name: String, f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => RT): Unit = ExpressionEvaluator.registerUdf(name, exprs => applyUdf(udf(f), exprs))

  def parseExpression(sqlText: String): Expression = {
    functions.expr(sqlText).expr
  }

  def resolveExpression(exprCol: Expression, schema: StructType): Expression = {
    ExpressionEvaluator.resolveExpression(exprCol, schema)
  }

  def applyUdf(udf: UserDefinedFunction, exprs: Seq[Expression]): Expression = {
    udf match {
      case udf: SparkUserDefinedFunction => udf.createScalaUDF(exprs)
      case udaf: UserDefinedAggregator[_, _, _] => udaf.scalaAggregator(exprs)
      case _ => throw new IllegalStateException(s"applyUdf is only implemented for SparkUserDefinedFunction and UserDefinedAggregator, but not for ${getClass.getSimpleName}")
    }
  }
}
