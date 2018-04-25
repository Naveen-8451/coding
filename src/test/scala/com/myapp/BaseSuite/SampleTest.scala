package com.myapp.BaseSuite

import org.junit._
import org.scalatest.FunSuite
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.sql.Connection
import java.sql.DriverManager
//import com.myapp.BaseSuite.BaseSuite

class SampleTest extends FunSuite {
  
  @Test
  def testSample():Unit={
    val path = "src/test/resources/data.csv"
    val dataDF = BaseSuite.sqlContext.read.format("csv").load(path)
    dataDF.show()
    
    
    
  }
  
  
}