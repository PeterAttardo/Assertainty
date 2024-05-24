package com.attardo.assertainty.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import scala.collection.Seq
import scala.jdk.CollectionConverters


fun Row.toList() : List<Any> = this.toSeq().toKotlinList()

fun <T> Seq<T>.toKotlinList() : List<T> =  CollectionConverters.SeqHasAsJava(this).asJava().toList()

fun <T> List<T>.toScalaSeq() : Seq<T> = CollectionConverters.ListHasAsScala(this).asScala().toSeq()

fun Dataset<Row>.toListofLists() : List<List<Any>> = this.collectAsList().toList().map(Row::toList)

fun Column.getName() : String = with(named()) {
    when (this) {
        is UnresolvedAlias -> this.aliasFunc().get().apply(expr())
        else -> name()
    }
}