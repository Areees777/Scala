def quitarAcentos(registros: DataFrame): DataFrame = {
    val docsDoradosSinLetra = registros.filter(length($"TXTO_DOCUMENTO") === "8" && (!substring($"TXTO_DOCUMENTO", 8, 1).isin(abecedarioUpper: _*) || !substring($"TXTO_DOCUMENTO", 8, 1).isin(abecedarioLower: _*)))            
    val calculoLetraDorada = docsDoradosSinLetra.withColumn("LetraCorrecta", when(substring($"TXTO_DOCUMENTO", 1, 1).geq(0) && substring($"TXTO_DOCUMENTO", 1, 1).leq(9), $"TXTO_DOCUMENTO"))            
    val letraDoradaCorrecta = calculoLetraDorada.withColumn("LetraCorrecta", when($"LetraCorrecta".isNotNull, limpiezaLetra)).filter($"LetraCorrecta".isNotNull && $"LetraCorrecta" =!= "INVALID_ID")            
    val preparacionLetraDorada = letraDoradaCorrecta.withColumn("MODULO", $"LetraCorrecta".mod(23).cast(IntegerType)).withColumn("LETRAS_DNI", lit(letraNIF))            
    val doradosConLetra = preparacionLetraDorada.withColumn("LETRA_DEL_DNI", expr("substring(LETRAS_DNI,MODULO+1,1)")).withColumn("DNI_FINAL", lit(concat($"TXTO_DOCUMENTO", $"LETRA_DEL_DNI"))).drop("LETRA_DEL_DNI").drop("LETRAS_DNI").drop("MODULO").drop("LetraCorrecta").drop("TXTO_DOCUMENTO").withColumnRenamed("DNI_FINAL", "TXTO_DOCUMENTO")
}
