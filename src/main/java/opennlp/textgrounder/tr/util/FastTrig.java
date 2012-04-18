/**
 *  Copyright (C) 2007 J4ME, 2010 Travis Brown
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package opennlp.textgrounder.tr.util;

/**
 * Provides faster implementations of inverse trigonometric functions.
 * Adapted from code developed by J4ME <http://code.google.com/p/j4me/>, some
 * of which was adapted from a Pascal implementation posted by Everything2
 * user Gorgonzola <http://everything2.com/index.pl?node_id=1008481>.
 *
 * @author Dean Browne
 * @author Randy Simon
 * @author Michael Ebbage
 * @author Travis Brown <travis.brown@mail.utexas.edu>
 */
public final class FastTrig {
  /**
   * Constant used in the <code>atan</code> calculation.
   */
  private static final double ATAN_CONSTANT = 1.732050807569;

  /**
   * Returns the arc cosine of an angle, in the range of 0.0 through <code>Math.PI</code>.
   * Special case:
   * <ul>
   *  <li>If the argument is <code>NaN</code> or its absolute value is greater than 1,
   *      then the result is <code>NaN</code>.
   * </ul>
   * 
   * @param a - the value whose arc cosine is to be returned.
   * @return the arc cosine of the argument.
   */
  public static double acos(double a)   {
    if (Double.isNaN(a) || Math.abs(a) > 1.0) {
      return Double.NaN;
    } else {
      return FastTrig.atan2(Math.sqrt(1.0 - a * a), a);
    }
  }

  /**
   * Returns the arc sine of an angle, in the range of <code>-Math.PI/2</code> through
   * <code>Math.PI/2</code>.  Special cases:
   * <ul>
   *  <li>If the argument is <code>NaN</code> or its absolute value is greater than 1,
   *      then the result is <code>NaN</code>.
   *  <li>If the argument is zero, then the result is a zero with the same sign
   *      as the argument.
   * </ul>
   * 
   * @param a - the value whose arc sine is to be returned.
   * @return the arc sine of the argument.
   */
  public static double asin(double a) {
    if (Double.isNaN(a) || Math.abs(a) > 1.0) {
      return Double.NaN;
    } else if (a == 0.0) {
      return a;
    } else {
      return FastTrig.atan2(a, Math.sqrt(1.0 - a * a));
    }
  }

  /**
   * Returns the arc tangent of an angle, in the range of <code>-Math.PI/2</code>
   * through <code>Math.PI/2</code>.  Special cases:
   * <ul>
   *  <li>If the argument is <code>NaN</code>, then the result is <code>NaN</code>.
   *  <li>If the argument is zero, then the result is a zero with the same 
   *      sign as the argument.
   * </ul>
   * <p>
   * A result must be within 1 ulp of the correctly rounded result.  Results
   * must be semi-monotonic.
   * 
   * @param a - the value whose arc tangent is to be returned. 
   * @return the arc tangent of the argument.
   */
  public static double atan(double a) {
    if (Double.isNaN(a) || Math.abs(a) > 1.0) {
      return Double.NaN;
    } else if (a == 0.0) {
      return a;
    } else {
      boolean negative = false;
      boolean greaterThanOne = false;
      int i = 0;

      if (a < 0.0) {
        a = -a;
        negative = true;
      }

      if (a > 1.0) {
        a = 1.0 / a;
        greaterThanOne = true;
      }

      for (double t = 0.0; a > Math.PI / 12.0; a *= t) {
        i++;
        t = 1.0 / (a + ATAN_CONSTANT);
        a *= ATAN_CONSTANT;
        a -= 1.0;
      }

      double arcTangent = a * (0.55913709
                            / (a * a + 1.4087812)
                            + 0.60310578999999997 
                            - 0.051604539999999997 * a * a);

      for (; i > 0; i--) {
        arcTangent += Math.PI / 6.0;
      }

      if (greaterThanOne) {
        arcTangent = Math.PI / 2.0 - arcTangent;
      }

      if (negative)
      {
        arcTangent = -arcTangent;
      }

      return arcTangent;
    }
  }
  
  /**
   * Converts rectangular coordinates (x, y) to polar (r, <i>theta</i>).  This method
   * computes the phase <i>theta</i> by computing an arc tangent of y/x in the range
   * of <i>-pi</i> to <i>pi</i>.  Special cases:
   * <ul>
   *  <li>If either argument is <code>NaN</code>, then the result is <code>NaN</code>.
   *  <li>If the first argument is positive zero and the second argument is
   *      positive, or the first argument is positive and finite and the second
   *      argument is positive infinity, then the result is positive zero.
   *  <li>If the first argument is negative zero and the second argument is
   *      positive, or the first argument is negative and finite and the second
   *      argument is positive infinity, then the result is negative zero.
   *  <li>If the first argument is positive zero and the second argument is 
   *      negative, or the first argument is positive and finite and the second
   *      argument is negative infinity, then the result is the <code>double</code> value 
   *      closest to <i>pi</i>.
   *  <li>If the first argument is negative zero and the second argument is 
   *      negative, or the first argument is negative and finite and the second
   *      argument is negative infinity, then the result is the <code>double</code> value
   *      closest to <i>-pi</i>.
   *  <li>If the first argument is positive and the second argument is positive
   *      zero or negative zero, or the first argument is positive infinity and
   *      the second argument is finite, then the result is the <code>double</code> value 
   *      closest to <i>pi</i>/2.
   *  <li>If the first argument is negative and the second argument is positive
   *      zero or negative zero, or the first argument is negative infinity and
   *      the second argument is finite, then the result is the <code>double</code> value
   *      closest to <i>-pi</i>/2.
   *  <li>If both arguments are positive infinity, then the result is the double
   *      value closest to <i>pi</i>/4.
   *  <li>If the first argument is positive infinity and the second argument is
   *      negative infinity, then the result is the double value closest to 3*<i>pi</i>/4.
   *  <li>If the first argument is negative infinity and the second argument is
   *      positive infinity, then the result is the double value closest to -<i>pi</i>/4.
   *  <li>If both arguments are negative infinity, then the result is the double
   *      value closest to -3*<i>pi</i>/4.
   * </ul>
   * <p>
   * A result must be within 2 ulps of the correctly rounded result.  Results
   * must be semi-monotonic.
   * 
   * @param y - the ordinate coordinate
   * @param x - the abscissa coordinate 
   * @return the <i>theta</i> component of the point (r, <i>theta</i>) in polar
   *   coordinates that corresponds to the point (x, y) in Cartesian coordinates.
   */
  public static double atan2(double y, double x) {
    if (Double.isNaN(y) || Double.isNaN(x)) {
      return Double.NaN;
    } else if (Double.isInfinite(y)) {
      if (y > 0.0) {
        if (Double.isInfinite(x)) {
          if (x > 0.0) {
            return Math.PI / 4.0;
          } else {
            return 3.0 * Math.PI / 4.0;
          }
        } else if (x != 0.0) {
          return Math.PI / 2.0;
        }
      }
      else {
        if (Double.isInfinite(x)) {
          if (x > 0.0) {
            return -Math.PI / 4.0;
          } else {
            return -3.0 * Math.PI / 4.0;
          }
        } else if (x != 0.0) {
          return -Math.PI / 2.0;
        }
      }
    } else if (y == 0.0) {
      if (x > 0.0) {
        return y;
      } else if (x < 0.0) {
        return Math.PI;
      }
    } else if (Double.isInfinite(x)) {
      if (x > 0.0) {
        if (y > 0.0) {
          return 0.0;
        } else if (y < 0.0) {
          return -0.0;
        }
      }
      else {
        if (y > 0.0) {
          return Math.PI;
        } else if (y < 0.0) {
          return -Math.PI;
        }
      }
    } else if (x == 0.0) {
      if (y > 0.0) {
        return Math.PI / 2.0;
      } else if (y < 0.0) {
        return -Math.PI / 2.0;
      }
    }

    // Implementation a simple version ported from a PASCAL implementation at
    // <http://everything2.com/index.pl?node_id=1008481>.
    double arcTangent;

    // Use arctan() avoiding division by zero.
    if (Math.abs(x) > Math.abs(y)) {
      arcTangent = FastTrig.atan(y / x);
    } else {
      arcTangent = FastTrig.atan(x / y); // -PI/4 <= a <= PI/4.

      if (arcTangent < 0.0) {
        // a is negative, so we're adding.
        arcTangent = -Math.PI / 2 - arcTangent;
      } else {
        arcTangent = Math.PI / 2 - arcTangent;
      }
    }

    // Adjust result to be from [-PI, PI]
    if (x < 0.0) {
      if (y < 0.0) {
        arcTangent = arcTangent - Math.PI;
      } else {
        arcTangent = arcTangent + Math.PI;
      }
    }

    return arcTangent;
  }
}

