package pagerank

import scala.xml._

object work {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  val a = <h1>123<br/>456</h1>                    //> a  : scala.xml.Elem = <h1>123<br/>456</h1>
	a.text                                    //> res0: String = 123456
	
	"dsdsa   dsdasdsa".replaceAll("\\s+", " ")//> res1: String = dsdsa dsdasdsa

}