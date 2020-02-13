// HANDLER DE LIMPIEZA DE CAMPOS (CARACTERES ESPECIALES Y LOWER)
  def formatearCampo(registros: DataFrame, campo: String): DataFrame = {

    val limpiezaCampo_0 = when(col(campo).isNotNull, regexp_replace(col(campo), "[\\!\"\\#\\$%&''\\(\\)\\*\\+'\\-\\/\\{\\|\\}\\~¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿÷]", ""))
    val limpiezaCampo_1 = when(col(campo).isNotNull, regexp_replace(col(campo), "[ÀÁÂÃÄÅÆ]", "A"))
    val limpiezaCampo_2 = when(col(campo).isNotNull, regexp_replace(col(campo), "[Ç]", "C"))
    val limpiezaCampo_3 = when(col(campo).isNotNull, regexp_replace(col(campo), "[ÈÉÊË]", "E"))
    val limpiezaCampo_4 = when(col(campo).isNotNull, regexp_replace(col(campo), "[ÌÍÎÏ]", "I"))
    val limpiezaCampo_5 = when(col(campo).isNotNull, regexp_replace(col(campo), "[Ð]", "D"))
    val limpiezaCampo_6 = when(col(campo).isNotNull, regexp_replace(col(campo), "[ÒÓÔÕÖ]", "O"))
    val limpiezaCampo_7 = when(col(campo).isNotNull, regexp_replace(col(campo), "[×]", "X"))
    val limpiezaCampo_8 = when(col(campo).isNotNull, regexp_replace(col(campo), "[Øø]", "0"))
    val limpiezaCampo_9 = when(col(campo).isNotNull, regexp_replace(col(campo), "[ÙÚÛÜÝ]", "U"))
    val limpiezaCampo_10 = when(col(campo).isNotNull, regexp_replace(col(campo), "[Þ]", "P"))
    val limpiezaCampo_11 = when(col(campo).isNotNull, regexp_replace(col(campo), "[ß]", "B"))
    val limpiezaCampo_12 = when(col(campo).isNotNull, regexp_replace(col(campo), "[àáâãäåæ]", "a"))
    val limpiezaCampo_13 = when(col(campo).isNotNull, regexp_replace(col(campo), "[ç]", "c"))
    val limpiezaCampo_14 = when(col(campo).isNotNull, regexp_replace(col(campo), "[èéêë]", "e"))
    val limpiezaCampo_15 = when(col(campo).isNotNull, regexp_replace(col(campo), "[ìíîï]", "i"))
    val limpiezaCampo_16 = when(col(campo).isNotNull, regexp_replace(col(campo), "[ðòóôõö]", "o"))
    val limpiezaCampo_17 = when(col(campo).isNotNull, regexp_replace(col(campo), "[ñ]", "n"))
    val limpiezaCampo_18 = when(col(campo).isNotNull, regexp_replace(col(campo), "[ùúûü]", "u"))
    val limpiezaCampo_19 = when(col(campo).isNotNull, regexp_replace(col(campo), "[ýÿ]", "y"))
    val limpiezaCampo_20 = when(col(campo).isNotNull, regexp_replace(col(campo), "[þ]", "p"))

    val camposLimpios_0 = registros.withColumn(campo, limpiezaCampo_0).withColumn(campo, limpiezaCampo_1).withColumn(campo, limpiezaCampo_2).withColumn(campo, limpiezaCampo_3).withColumn(campo, limpiezaCampo_4).withColumn(campo, limpiezaCampo_5).withColumn(campo, limpiezaCampo_6)
    camposLimpios_0.persist(StorageLevel.MEMORY_ONLY_SER)

    val camposLimpios_1 = camposLimpios_0.withColumn(campo, limpiezaCampo_7).withColumn(campo, limpiezaCampo_8).withColumn(campo, limpiezaCampo_9).withColumn(campo, limpiezaCampo_10).withColumn(campo, limpiezaCampo_11).withColumn(campo, limpiezaCampo_12)
    camposLimpios_1.persist(StorageLevel.MEMORY_ONLY_SER)

    val camposLimpios_2 = camposLimpios_1.withColumn(campo, limpiezaCampo_13).withColumn(campo, limpiezaCampo_14).withColumn(campo, limpiezaCampo_15).withColumn(campo, limpiezaCampo_16)
    camposLimpios_2.persist(StorageLevel.MEMORY_ONLY_SER)

    val camposLimpios_3 = camposLimpios_2.withColumn(campo, limpiezaCampo_17).withColumn(campo, limpiezaCampo_18).withColumn(campo, limpiezaCampo_19).withColumn(campo, limpiezaCampo_20)

    val registros_formateados = camposLimpios_3.withColumn(campo, lower(col(campo)))
    registros_formateados.persist(StorageLevel.MEMORY_ONLY_SER)

    return registros_formateados

  }
