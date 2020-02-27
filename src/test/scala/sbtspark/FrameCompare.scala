package sbtspark

import org.apache.spark.sql.DataFrame

class FrameCompare {
//Function two compare frames
  def areEqual(frame1: DataFrame, frame2: DataFrame): Boolean = {
    if (frame1.except(frame2).count() == 0) {
      true
    } else {
      false
    }
  }
}
