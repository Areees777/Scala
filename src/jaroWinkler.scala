object SimilarityFunctions {
  
  private def withExpr(expr: Expression): Column = new Column(expr)

  val jaro_winkler = udf[Option[Double], String, String](jaroWinlkerFun)

  def jaroWinlkerFun(s1: String, s2: String): Option[Double] = {
    val str1 = Option(s1).getOrElse(return None)
    val str2 = Option(s2).getOrElse(return None)
    val j = new JaroWinklerDistance()
    Some(j.apply(str1, str2))
  }

}
