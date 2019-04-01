import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import com.sgcharts.badrenter.Smote
import org.apache.spark.sql.DataFrame

class SmoteSpec extends FlatSpec with SharedSparkContext {
  override implicit def reuseContextIfPossible: Boolean = true

  "Child sample" should "take either parent's value in a discrete string attribute" in {
    pending
    /*Smote(
      sample = sc.cr,
      discreteStringAttributes = Seq[String]("string_col"),
      discreteLongAttributes = Seq.empty[String],
      continuousAttributes = Seq.empty[String],
      bucketLength = 1
    )(sc)*/


  }

}
