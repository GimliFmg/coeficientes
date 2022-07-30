package commons

object Literales {

  val pathRegistroEquipos: String = "/Users/fmgallego/Desktop/MIXTO/coef/src/main/resources/torneo/"

  object Columnas {

    val IdGrupoCol: String = "GRUPO"
    val IdPartidoCol: String = "PARTIDO"
    val IdEquipo1Col: String = "EQUIPO 1"
    val IdEquipoCol: String = "IDEQUIPO"
    val EquipoCol: String = "EQUIPO"
    val IdEquipo2Col: String = "EQUIPO 2"
    val PuntosEquipo1Col: String = "PUNTOS EQUIPO 1"
    val PuntosEquipo2Col: String = "PUNTOS EQUIPO 2"
    val PuntosAFavorCol: String = "PUNTOS A FAVOR"
    val PuntosEnContraCol: String = "PUNTOS EN CONTRA"
    val WinFlag: String = "WINFLAG"
    val PartidosGanadosCol: String = "PARTIDOS GANADOS"
    val CoeficienteEquipoCol: String = "COEFICIENTE"
    val ClafisicacionGrupoCol: String = "CLASIFICACION"
    val PkJoin: String = "PK_JOIN"
    val VersusCol: String = "VS"
    val RowNumber: String = "ROWNUM"
  }

  object LogInformation {

    val ExplicacionClasificacion = "Por cada grupo, clasifican directamente a octavos de final los 3 primeros equipos clasificados " +
      "y el mejor 4o de entre todos los grupos. \n" +
      "Para saber qué equipo está clasificado, primarán las victorias conseguidas. \n" +
      "En caso de existir un empate de partidos ganados, se decidirá el equipo que clasifica mejor en función de el coeficiente, \n" +
      "es decir, de la suma de sus puntos a favor dividido entre la suma de sus puntos en contra. Esta columna aparece en las tablas como " +
      "'COEFICIENTE'. \n MUCHA SUERTE A TODOS =) \n \n"

    val ExplicacionCruces = "Para conocer los cruces, en este caso primará el coeficiente. \n Cuanto mejor coeficiente haya conseguido un equipo, más posibilidad " +
      "tendrá de enfrentarse a un equipo con peor coeficiente. \n Para conocer los cruces, se ordena la lista por coeficiente conseguido, de mayor a menor, \n enfrentándose " +
      "el primer clasificado en esta tabla ordenada contra el último de la misma. \n \n"
  }
}
