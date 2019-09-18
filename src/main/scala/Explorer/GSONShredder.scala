package Explorer

import com.google.gson.Gson
import scala.collection.JavaConverters._

object GSONShredder {

  /** Preforms the initial serialization step
    * This dataframe is then cached and can be used during clustering for correlation
    * Also handles the initial conversion to our JsonExplorer types
    * @deprecated
    *
    */

  def serialize(rows: Iterator[String]): Iterator[JsonExplorerType] = {
    val StringClass = classOf[String]
    val DoubleClass = classOf[java.lang.Double]
    val BooleanClass = classOf[java.lang.Boolean]
    val ArrayClass = classOf[java.util.ArrayList[_]]
    val ObjectClass = classOf[com.google.gson.internal.LinkedTreeMap[_,_]]
    val MapType = new java.util.HashMap[String,Object]().getClass
    val ArrayType = new java.util.ArrayList[Object]().getClass


    val gson = new Gson()
    // local collector, collects attributeName and type and it's count.
    val mapped_rows: scala.collection.mutable.ListBuffer[JsonExplorerType] = new scala.collection.mutable.ListBuffer[JsonExplorerType]()

    // this is the recursive function that is responsible for updating the collector, can probably be replace instead of
    def extractTypesO(jsonMap: java.util.Map[String,Object]): JsonExplorerType = {

      val local_objs: scala.collection.mutable.HashMap[String,JsonExplorerType] = new scala.collection.mutable.HashMap[String,JsonExplorerType]()

      jsonMap.asScala.foreach{case(name,obj) => {
        var attributeClass: Class[_ <: Object] = null
        try {
          attributeClass = obj.getClass
        } catch {
          case e: java.lang.NullPointerException => // do nothing
        }
        attributeClass match {
          case(StringClass) => local_objs.put(name,JE_String)
          case(DoubleClass) => local_objs.put(name,JE_Numeric)
          case(BooleanClass) => local_objs.put(name,JE_Boolean)
          case(null) => local_objs.put(name,JE_Null)
          case(ArrayClass) =>
            val attributeList = obj.asInstanceOf[java.util.ArrayList[java.lang.Object]]
            if(attributeList.isEmpty)
              local_objs.put(name,JE_Empty_Array)
            else{
              local_objs.put(name,extractTypesA(attributeList))
            }
          case(ObjectClass) =>
            val t = gson.fromJson(gson.toJson(obj), MapType)
            if(t.isEmpty)
              local_objs.put(name,JE_Empty_Object)
            else{
              local_objs.put(name,extractTypesO(gson.fromJson(gson.toJson(obj), MapType)))
            }
          case _ =>
            throw new UnknownTypeException("Unknown Type Found in Extractor MapType: " + attributeClass.toString())
        } // end match
      }}

      return JE_Object(local_objs)

    }


    def extractTypesA(jsonList: java.util.ArrayList[Object]): JsonExplorerType = {

      val local_objs: scala.collection.mutable.ListBuffer[JsonExplorerType] = new scala.collection.mutable.ListBuffer[JsonExplorerType]()

      jsonList.asScala.foreach{obj => {
        var attributeClass: Class[_ <: Object] = null
        try {
          attributeClass = obj.getClass
        } catch {
          case e: java.lang.NullPointerException => // do nothing
        }
        attributeClass match {
          case(StringClass) => local_objs.append(JE_String)
          case(DoubleClass) => local_objs.append(JE_Numeric)
          case(BooleanClass) => local_objs.append(JE_Boolean)
          case(null) => local_objs.append(JE_Null)
          case(ArrayClass) =>
            val attributeList = obj.asInstanceOf[java.util.ArrayList[java.lang.Object]]
            if(attributeList.isEmpty)
              local_objs.append(JE_Empty_Array)
            else{
              local_objs.append(extractTypesA(attributeList))
            }
          case(ObjectClass) =>
            val t = gson.fromJson(gson.toJson(obj), MapType)
            if(t.isEmpty)
              local_objs.append(JE_Empty_Object)
            else{
              local_objs.append(extractTypesO(gson.fromJson(gson.toJson(obj), MapType)))
            }
          case _ =>
            throw new UnknownTypeException("Unknown Type Found in Extractor MapType: " + attributeClass.toString())
        } // end match
      }}

      return JE_Array(local_objs)
    }

    while(rows.hasNext){
      val row = rows.next()
      val map: java.util.HashMap[String,Object] = gson.fromJson(row, MapType)
      mapped_rows.append(extractTypesO(map)) // this call does the updating as well
      //println(mapped_rows.size)
      // new row
    }
    return mapped_rows.iterator
  }

}
