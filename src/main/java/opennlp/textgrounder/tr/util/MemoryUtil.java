/*
 * This class contains tools for checking memory usage during runtime.
 */

package opennlp.textgrounder.tr.util;

public class MemoryUtil {
  public static long getMemoryUsage(){
    takeOutGarbage();
    long totalMemory = Runtime.getRuntime().totalMemory();

    takeOutGarbage();
    long freeMemory = Runtime.getRuntime().freeMemory();

    return (totalMemory - freeMemory);
  }

  private static void takeOutGarbage() {
    collectGarbage();
    collectGarbage();
  }

  private static void collectGarbage() {
    try {
      System.gc();
      Thread.currentThread().sleep(100);
      System.runFinalization();
      Thread.currentThread().sleep(100);
    }
    catch (Exception ex){
      ex.printStackTrace();
    }
  }
}
