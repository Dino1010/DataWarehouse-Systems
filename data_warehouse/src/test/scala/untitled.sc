import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.math.NumberUtils

val line = "INSERT INTO `t_md_areas` VALUES (621224204, '迷坝乡', 621224, '', 4, 1, 105.77948108064203, 33.54050597681045, 105.78336, 33.53879, 105.7897757892442, 33.5256544949124);"

val arr = line.split( " ")
println(arr(4).substring(1, arr(4).length - 1))

class MyStr(str:String){
  def str2Double:Double={
    str.toDouble
  }
}


implicit def str2MyStr(str:String) = new MyStr(str)

println("1".str2Double)

println(NumberUtils.isParsable("123.a5"))