object DataReaderFactory {
  def createReader(format: String, path: String, prop: Map[String, String] = Map.empty): DataReader = {
    format.toLowerCase match {
      case "csv" => LoadCSV(path, prop)
      case _ => throw new IllegalArgumentException(s"Unsupported format: $format")
    }
  }
}
