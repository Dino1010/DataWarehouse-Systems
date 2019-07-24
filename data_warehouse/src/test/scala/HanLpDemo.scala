import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.dictionary.py.Pinyin
import com.hankcs.hanlp.seg.common.Term

object HanLpDemo {

  def main(args: Array[String]): Unit = {

    val pinyins: util.List[Pinyin] = HanLP.convertToPinyinList("我有一头小毛驴我从来也不骑，有一天我心血来潮骑着去赶集")

    import scala.collection.JavaConversions._

    pinyins.foreach(pinyin =>  print(pinyin.getPinyinWithoutTone+" " ))


    println()

    val str = HanLP.convertToTraditionalChinese("我爱你祖国，中华人民共和国万岁")
    println(str)


    val simp = HanLP.convertToSimplifiedChinese("我愛我的同胞，台灣人民也是我們的兄弟，知識豐富，但是現在被一幫台獨分子所挾持，我們一定要解放台灣，拯救台灣人民於水火")
    println(simp)

    val keywords = HanLP.extractKeyword("公开报道还显示，今年4月，阿里获评国际性的“知识产权保护和科技创新奖”。5月，阿里因最高标准保护知识产权获评2019年度“亚洲太平洋地区最佳团队”，也是该奖设立11年来唯一获奖的中国企业。6月，欧洲刑警组织公开表示，阿里在打假方面做得系统而全面，技术能力值得借鉴。7月，迪拜警方来访阿里希望加入阿里打假联盟。\n\n在打假这件事上，阿里试图团结一切可以团结的力量，建立打假共治系统。在阿里的推动下，公共领域的讨论、监管的重视也加速了中国知识产权保护的进程！中国率先形成执法机关、品牌权利人、平台联动的打假共治系统，这一良性循环的生态系统也在不断发挥作用。",10)
    println(keywords)

    val sum = HanLP.extractSummary("海外网7月23日电 据外交部驻香港特派员公署消息，2019年7月23日，针对美国务院发言人22日就香港问题发表言论，外交部驻香港公署发言人表示，我们对美方错误表态表示强烈不满和坚决反对。回归以来，“一国两制”、“港人治港”、高度自治得到切实贯彻落实。港人依法享有前所未有的广泛权利与自由，这是有目共睹的事实。美方所谓香港自治受到“侵蚀”的表态，是基于偏见的无端指责，是别有用心的政治抹黑。\n\n发言人强调，近期香港社会发生的一系列极端暴力行为，严重冲击香港法治根基，严重威胁社会安全与秩序，践踏“一国两制”的底线。我们再次敦促美方摒弃双重标准，立即停止向暴力不法行为发出任何错误信号，立即停止以任何借口干预香港事务和中国内政。",3)
    println(sum)


    val terms: util.List[Term] = HanLP.segment("我有一头小毛驴我从来也不骑，有一天我心血来潮骑着去赶集")
    println(terms.map(t => t.word).filter(_.size>1))

  }

}
