import io.archivesunleashed._
import io.archivesunleashed.matchbox._

RecordLoader.loadArchives("/mnt/vol1/data_sets/geocities/warcs/*.gz", sc)
  .keepValidPages()
  .keepUrlPatterns(Set("(?i)http://geocities.yahoo.com/EnchantedForest/.*".r))
  .map(r => (r.getCrawlDate, r.getDomain, r.getUrl, RemoveHTML(RemoveHttpHeader(r.getContentString))))
  .saveAsTextFile("/mnt/vol1/derivative_data/geocities/text/enchantedforest/enchantedforest-noheaders")