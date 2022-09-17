package com._4paradigm.openmldb.batch_test.util

import com._4paradigm.openmldb.batch.api.{OpenmldbDataframe, OpenmldbSession}
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult
import com._4paradigm.openmldb.test_common.model.{InputDesc, SQLCase, TableFile}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataTypes, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

object OpenMLDBBatchUtil {
    private val logger = LoggerFactory.getLogger(this.getClass)
    def executeBatch(sqlCase: SQLCase): OpenMLDBResult ={
        val openMLDBResult = new OpenMLDBResult()
        val sparkSession = SparkSession.builder().master("local[4]").getOrCreate()
        val openMLDBSession = new OpenmldbSession(sparkSession)
        var table_id = 0
        val inputNames = mutable.ListBuffer[(Int, String)]()
        // 加载数据并注册成表
        sqlCase.getInputs.asScala.foreach(input => {
            val (name, df) = loadInputData(sparkSession,input)
            OpenmldbDataframe(openMLDBSession, df).createOrReplaceTempView(name)
            inputNames += Tuple2[Int, String](table_id, name)
            table_id += 1
        })
        val sql = sqlCase.getSql
        openMLDBResult.setSql(sql)
        try {
            val df = openMLDBSession.sql(sql).sparkDf
            df.cache()
            parseDataFrame(df, openMLDBResult)
            openMLDBResult.setOk(true)
            openMLDBResult.setMsg("success")
        }catch {
            case e:Exception =>{
                e.printStackTrace()
                openMLDBResult.setOk(false)
                openMLDBResult.setMsg(e.getMessage)
            }
        }
        openMLDBResult
    }

    def loadInputData(sparkSession:SparkSession,inputDesc: InputDesc): (String, DataFrame) = {
        val sess = sparkSession
        val schema = parseSchema(inputDesc.getColumns)
        val data = parseData(inputDesc.getRows, schema)
            .map(arr => Row.fromSeq(arr)).toList.asJava
        val df = sess.createDataFrame(data, schema)
        inputDesc.getName -> df
    }
    def parseSchema(columns: java.util.List[String]): StructType = {
        val parts = columns.toArray.map(_.toString()).map(_.trim).filter(_ != "").map(_.reverse.replaceFirst(" ", ":").reverse.split(":"))
        parseSchema(parts)
    }


    def parseSchema(parts: Array[Array[String]]): StructType = {
        val fields = parts.map(part => {
            val colName = part(0)
            val typeName = part(1)
            val dataType = typeName match {
                case "i16" => ShortType
                case "int16" => ShortType
                case "smallint" => ShortType
                case "int" => IntegerType
                case "i32" => IntegerType
                case "int32" => IntegerType
                case "i64" => LongType
                case "long" => LongType
                case "bigint" => LongType
                case "int64" => LongType
                case "float" => FloatType
                case "double" => DoubleType
                case "string" => StringType
                case "timestamp" => TimestampType
                case "date" => DateType
                case "bool" => BooleanType
                case _ => throw new IllegalArgumentException(
                    s"Unknown type name $typeName")
            }
            StructField(colName, dataType)
        })
        StructType(fields)
    }

    def parseData(rows: java.util.List[java.util.List[Object]], schema: StructType): Array[Array[Any]] = {
        val data = rows.asScala.map(_.asScala.map(x => if (null == x) "null" else x.toString).toArray).toArray
        parseData(data, schema)
    }

    def parseData(rows: Array[Array[String]], schema: StructType): Array[Array[Any]] = {

        rows.flatMap(parts => {
            if (parts.length != schema.size) {
                logger.error(s"Broken line: $parts")
                None
            } else {
                Some(schema.zip(parts).map {
                    case (field, str) =>
                        if (str == "NULL" || str == "null") {
                            null
                        } else {
                            field.dataType match {
                                case ByteType => str.trim.toByte
                                case ShortType => str.trim.toShort
                                case IntegerType => str.trim.toInt
                                case LongType => str.trim.toLong
                                case FloatType => toFloat(str)
                                case DoubleType => toDouble(str)
                                case StringType => str
                                case TimestampType => new Timestamp(str.trim.toLong)
                                case DateType =>
                                    new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str.trim + " 00:00:00").getTime)
                                case BooleanType => str.trim.toBoolean
                                case _ => throw new IllegalArgumentException(
                                    s"Unknown type ${field.dataType}")
                            }
                        }
                }.toArray)
            }
        })
    }

    def toFloat(value: Any): Float = {
        value match {
            case f: Float => f
            case s: String if s.toLowerCase == "nan" => Float.NaN
            case s: String => s.trim.toFloat
            case _ => value.toString.toFloat
        }
    }

    def toDouble(value: Any): Double = {
        value match {
            case f: Double => f
            case s: String if s.toLowerCase == "nan" => Double.NaN
            case s: String => s.trim.toDouble
            case _ => value.toString.toDouble
        }
    }

    def parseDataFrame(df: DataFrame,openMLDBResult:OpenMLDBResult): Unit ={
        val columnNames = new util.ArrayList[String]()
        val columnTypes = new util.ArrayList[String]()
        df.schema.foreach(sf=>{
            columnNames.add(sf.name)
            columnTypes.add(sf.dataType.typeName)
        })
        openMLDBResult.setColumnNames(columnNames)
        openMLDBResult.setColumnTypes(columnTypes)
        val rows:util.List[util.List[Object]] = df.collect().map(row=>parseRow(row,df.schema.fields)).toList.asJava
        openMLDBResult.setResult(rows)
    }
    def parseRow(row:Row,fields:Array[StructField]): util.List[Object] ={
        val list = new util.ArrayList[Object]()
        for(i <- 0 until  fields.length){
            val dateType = fields(i).dataType
            var data:Object = null
            if(dateType == DataTypes.StringType){
                data = row.getString(i)
            }else if(dateType == DataTypes.ShortType){
                data = row.getShort(i)
            }else if(dateType == DataTypes.IntegerType){
                data = row.getInt(i)
            }else if(dateType == DataTypes.LongType){
                data = row.getLong(i)
            }else if(dateType == DataTypes.FloatType){
                data = row.getFloat(i)
            }else if(dateType == DataTypes.DoubleType){
                data = row.getDouble(i)
            }else if(dateType == DataTypes.DateType){
                data = row.getDate(i)
            }else if(dateType == DataTypes.TimestampType){
                data = row.getTimestamp(i)
            }else if(dateType == DataTypes.BooleanType){
                data = row.getBoolean(i)
            }
            list.add(data)
        }
        list
    }
}
