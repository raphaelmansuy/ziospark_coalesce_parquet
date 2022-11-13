package elitizon.ziospark.coalesce

object FormatUtil {

  // format numeric with separator every 3 digits
  def formatNumber(number: Long): String = {
    val formatter = java.text.NumberFormat.getIntegerInstance
    formatter.format(number)
  }

  // format size in MB, GB, TB depending on the size
  // with separator between thousands
  def formatSize(size: Long): String = {
    if (size < 1024) {
      formatNumber(size) + " B"
    } else if (size < 1024 * 1024) {
      formatNumber(size / 1024) + " KB"
    } else if (size < 1024 * 1024 * 1024) {
      formatNumber(size / (1024 * 1024)) + " MB"
    } else {
      formatNumber(size / (1024 * 1024 * 1024)) + " GB"
    }
  }

}
