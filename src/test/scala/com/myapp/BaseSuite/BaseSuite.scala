package com.myapp.BaseSuite

import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.junit.AfterClass
import org.apache.spark.SparkContext
import org.apache.derby.jdbc.EmbeddedDriver
import java.sql.DriverManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext



@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array())
class BaseSuite {
  println("test BaseSuite")
  
}
object BaseSuite{
  
  println("test object BaseSuite")
  val sparkContext:SparkContext = getSparkContext()
  val hiveContext:HiveContext=getHiveContext()
  val sqlContext:SQLContext = new SQLContext(sparkContext)
  
  @AfterClass
  def tearDownClass:Unit={
    BaseSuite.sparkContext.stop()
    
  }
  def getAbsolutePath(path:String):String={
    var currentDirectory:String=System.getProperty("user.dir");
    if(System.getProperty("os.name").contains("windows")){
      currentDirectory = currentDirectory.replaceAll("C:","");
      currentDirectory = currentDirectory.replace("\\:","/");
      
    }
    val finalDirectory:String = path.replaceFirst("/file/","/file" + currentDirectory + "/")
    println(finalDirectory)
    finalDirectory
    
  }
  
  def getSparkContext():SparkContext ={
    val dbName = "SparkDB"
    if(System.getProperty("os.name").contains("windows")){
      System.setProperty("hadoop.home.dir", "C:/Hadoop/winutils/")
      System.setProperty("hive.exec.scratchdir", "C:\\winutils\\hive")
      System.setProperty("hive.exec.scratch.dir.permission", "777")
    }
    val createURL = "jdbc:derby:memory:" + dbName + ";create=true"
    System.setProperty("javax.jdo.option.ConnectionURL", "createURL")
    System.setProperty("Javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
    
    val driver = new EmbeddedDriver
    DriverManager.registerDriver(driver)
    DriverManager.getConnection(createURL)
    
    return new SparkContext(new SparkConf()
        .setMaster("local[*]")
        .setAppName("BaseSuite")
        .set("javax.jdo.option.ConnectionURL", "createURL")
        .set("Javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
        .set("spark.testing.memory","2147480000"))
    
  }
  def getHiveContext():HiveContext={
    val hc = new org.apache.spark.sql.hive.HiveContext(sparkContext)
    val absolutePath = new java.io.File("target/HiveWH").getCanonicalPath
    println("Hive WH Path for tests: "+ absolutePath)
    hc.setConf("hive.metastore.warehouse.dir", absolutePath)
    hc.setConf("hive.metstore.metadb.dir", absolutePath)
    hc
  }
  
}