package coeficientes

import commons.FileReaderWriter._
import commons.Literales.Columnas._
import commons.Literales.pathRegistroEquipos
import commons.Literales.LogInformation.{ExplicacionClasificacion, ExplicacionCruces}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, concat, desc, lit, round, row_number, sum, when}

object MainRunner extends App with Logging {

  if (MainRunner().checkRegistro()) logger.error("ERROR EN EL REGISTRO DE PARTIDOS")
  else MainRunner().runner()
}

case class MainRunner() extends Logging {

  def runner() = {
    spark.sparkContext.setLogLevel("ERROR")
    val FinalDf: DataFrame = createGroupWithCoeficient()

    val FinalDfLogs = FinalDf.select(ClafisicacionGrupoCol, IdGrupoCol, IdEquipoCol,
      PartidosGanadosCol, CoeficienteEquipoCol, PuntosAFavorCol, PuntosEnContraCol)
    logWithGroupsClassification(FinalDfLogs)

    logWithClassifiedTeamsByPos(FinalDfLogs)

    val EquiposClasificados: DataFrame =
      getClassifiedTeams(
        FinalDf,
        listaPuestosClasificados = List(1, 2, 3),
        tercerOCuartoMejor = 4,
        limiteTercerOCuarto = 1)

    val CrucesOctavos: DataFrame = getCruces(EquiposClasificados)

    println(ExplicacionClasificacion)
    println("## LA TABLA DE EQUIPOS CLASIFICADOS PARA OCTAVOS ES: \n")
    println(EquiposClasificados.show(false))

    println(ExplicacionCruces)
    println("# LOS CRUCES PARA LA FASE DE OCTAVOS DE FINAL SON: \n")
    println(CrucesOctavos.show(false))

  }

  implicit val spark: SparkSession = getSparkSession

  val registroDF: DataFrame = reader(pathRegistroEquipos, spark)

  val Equipo1: DataFrame = registroDF
    .withColumn(IdEquipoCol, col(IdEquipo1Col))
    .withColumn(PuntosAFavorCol, col(PuntosEquipo1Col))
    .withColumn(PuntosEnContraCol, col(PuntosEquipo2Col))
    .select(IdGrupoCol, IdEquipoCol, PuntosAFavorCol, PuntosEnContraCol)
    .withColumn(WinFlag, when(col(PuntosAFavorCol) - col(PuntosEnContraCol) > 0, 1)
      .otherwise(0))

  val Equipo2: DataFrame = registroDF
    .withColumn(IdEquipoCol, col(IdEquipo2Col))
    .withColumn(PuntosAFavorCol, col(PuntosEquipo2Col))
    .withColumn(PuntosEnContraCol, col(PuntosEquipo1Col))
    .select(IdGrupoCol, IdEquipoCol, PuntosAFavorCol, PuntosEnContraCol)
    .withColumn(WinFlag, when(col(PuntosAFavorCol) - col(PuntosEnContraCol) > 0, 1)
      .otherwise(0))

  val DfToAggregate: DataFrame = (Equipo1 union Equipo2)
    .orderBy(IdGrupoCol, IdEquipoCol)

  def checkRegistro(): Boolean = {
    val CountPartidos = registroDF.count()
    val countPartidosPorGrupo = registroDF.groupBy(IdGrupoCol).count().orderBy(IdGrupoCol)
    val countPartidosPorGrupoEquipo = DfToAggregate
      .groupBy(IdGrupoCol, IdEquipoCol).count().orderBy(IdGrupoCol, IdEquipoCol)
    if (CountPartidos != 75) {
      logger.error(s"HAS INTRODUCIDO UN NUMERO DE PARTIDOS INCORRECTO ($CountPartidos), REVISA EL REGISTRO")
      logger.error(s"PARTIDOS POR GRUPO \n ${countPartidosPorGrupo.show(false)}")
      logger.error(s"PARTIDOS POR GRUPO Y EQUIPO \n ${
        countPartidosPorGrupoEquipo
          .filter(col("count") =!= 5).show(false)
      }")
      true
    }
    else false
  }

  def logWithGroupsClassification(FinalDf: DataFrame): Unit = {
    val Length = FinalDf.select(IdGrupoCol).distinct().count()
    val ListaGrupos: List[Long] = List.range(1, Length + 1)
    for (grupo <- List("A", "B", "C", "D", "E")) {
      println(s"### TABLA DE CLASIFICACIÓN DEL GRUPO $grupo \n")
      println(createGroupWithCoeficient(filterFlag = true, grupo).show(false))
    }
  }

  def logWithClassifiedTeamsByPos(FinalDf: DataFrame): Unit = {
    val Length: List[Int] = List(1, 2, 3, 4)
    for (clasificado <- Length) {
      val TextoClasificados= clasificado match {
        case 1 => "### LOS MEJORES PRIMEROS SON: "
        case 2 => "### LOS MEJORES SEGUNDOS SON: "
        case 3 => "### LOS MEJORES TERCEROS SON: "
        case 4 => "### EL MEJOR CUARTO ES: "
      }
      println(TextoClasificados + "\n")
      println(FinalDf.filter(col(ClafisicacionGrupoCol) === clasificado)
        .withColumn(EquipoCol, concat(col(IdGrupoCol), col(IdEquipoCol)))
        .select(EquipoCol, PartidosGanadosCol, CoeficienteEquipoCol,
          PuntosAFavorCol, PuntosEnContraCol)
        .orderBy(desc(PartidosGanadosCol), desc(CoeficienteEquipoCol))
        .limit(if (clasificado == 4) 1 else 5)
        .show(false))
    }
  }

  /**
    * It Creates a group aggregated
    *
    */
  def createGroupWithCoeficient(filterFlag: Boolean = false, groupId: String = ""): DataFrame = {

    val AggregateByGroup =
      DfToAggregate
        .groupBy(IdGrupoCol, IdEquipoCol)
        .agg(
          sum(col(PuntosAFavorCol)) as PuntosAFavorCol,
          sum(col(PuntosEnContraCol)) as PuntosEnContraCol,
          sum(col(WinFlag)) as PartidosGanadosCol)

    val WindowForRowNumber = Window.partitionBy(col(IdGrupoCol))
      .orderBy(desc(PartidosGanadosCol), desc(CoeficienteEquipoCol))

    val FilteringByGroup =
      AggregateByGroup
        .withColumn(CoeficienteEquipoCol,
          round(col(PuntosAFavorCol) / col(PuntosEnContraCol), 3))
        .withColumn(ClafisicacionGrupoCol, row_number().over(WindowForRowNumber))

    if (filterFlag) FilteringByGroup.filter(col(IdGrupoCol) === groupId)
    else FilteringByGroup

  }

  def getClassifiedTeams(FinalDf: DataFrame,
                         listaPuestosClasificados: List[Int],
                         tercerOCuartoMejor: Int,
                         limiteTercerOCuarto: Int): DataFrame = {
    val BestThirdFourthTeam: DataFrame =
      FinalDf
        .filter(col(ClafisicacionGrupoCol) === tercerOCuartoMejor)
        .orderBy(desc(PartidosGanadosCol), desc(CoeficienteEquipoCol))
        .limit(limiteTercerOCuarto)

    val Ordered = FinalDf
      .filter(col(ClafisicacionGrupoCol).isin(listaPuestosClasificados: _*))
      .union(BestThirdFourthTeam)
      .orderBy(desc(PartidosGanadosCol), desc(CoeficienteEquipoCol))
      .withColumn("R", lit("R"))
      .withColumn(ClafisicacionGrupoCol, row_number().over(Window.partitionBy("R").orderBy("R")))
      .withColumn(EquipoCol, concat(col(IdGrupoCol), col(IdEquipoCol)))
      .select(ClafisicacionGrupoCol, EquipoCol, PartidosGanadosCol, CoeficienteEquipoCol,
        PuntosAFavorCol, PuntosEnContraCol)
      .orderBy(col(ClafisicacionGrupoCol))

    Ordered

  }

  def getCruces(clasificados: DataFrame, limiteOctavos: Int = 8): DataFrame = {
    val WindowForJoin = Window
      .partitionBy(col(RowNumber))
      .orderBy(lit("R"))

    val MejoresDf: DataFrame = clasificados
      .withColumn(RowNumber, lit(RowNumber))
      .orderBy(desc(CoeficienteEquipoCol))
      .withColumn(PkJoin, row_number().over(WindowForJoin))
      .select(EquipoCol, CoeficienteEquipoCol, PkJoin)
      .limit(limiteOctavos)
      .withColumn(VersusCol, lit(VersusCol))

    val PeoresDf: DataFrame = clasificados
      .withColumn(RowNumber, lit(RowNumber))
      .orderBy(asc(CoeficienteEquipoCol))
      .withColumn(PkJoin, row_number().over(WindowForJoin))
      .select(EquipoCol, CoeficienteEquipoCol, PkJoin)
      .limit(limiteOctavos)

    val CrucesDf: DataFrame = MejoresDf.join(PeoresDf, Seq(PkJoin), "left")

    CrucesDf.drop(PkJoin)

  }
}
