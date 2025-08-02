package org.apache.spark.sql.custom

import ch.zzeekk.spark.expressions.ExpressionEvaluatorFactory
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedAggregator, UserDefinedFunction}
import org.apache.spark.sql.functions

import scala.reflect.ClassTag
import scala.reflect.runtime.universe


object SparkExpressionEvaluatorFactory extends ExpressionEvaluatorFactory[Expression] {

  override def getEvaluator[T <: Product : universe.TypeTag, R: universe.TypeTag : ClassTag](expression: String): ExpressionEvaluator[T, R] = {
    new ExpressionEvaluator[T,R](parseExpression(expression))
  }

  override def registerUdf(name: String, udfBuilder: Seq[Expression] => Expression): Unit = {
    ExpressionEvaluator.registerUdf(name, udfBuilder)
  }

  def parseExpression(sqlText: String): Expression = {
    functions.expr(sqlText).expr
  }

  def applyUdf(udf: UserDefinedFunction, exprs: Seq[Expression]): Expression = {
    udf match {
      case udf: SparkUserDefinedFunction => udf.createScalaUDF(exprs)
      case udaf: UserDefinedAggregator[_, _, _] => udaf.scalaAggregator(exprs)
      case _ => throw new IllegalStateException(s"applyUdf is only implemented for SparkUserDefinedFunction and UserDefinedAggregator, but not for ${getClass.getSimpleName}")
    }
  }
}
