///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Travis Brown, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import opennlp.textgrounder.util.Span;

public class StringEditMapper extends EditMapper<String> {
  public StringEditMapper(List<String> s, List<String> t) {
    super(s, t);
  }

  @Override
  protected int delCost(String x) {
    return x.length();
  }

  @Override
  protected int insCost(String x) {
    return x.length();
  }

  @Override
  protected int subCost(String x, String y) {
    int[][] ds = new int[x.length() + 1][y.length() + 1];
    for (int i = 0; i <= x.length(); i++) { ds[i][0] = i; }
    for (int j = 0; j <= y.length(); j++) { ds[0][j] = j; }

    for (int i = 1; i <= x.length(); i++) {
      for (int j = 1; j <= x.length(); j++) {
        int del = ds[i - 1][j] + 1;
        int ins = ds[1][j - 1] + 1;
        int sub = ds[i - 1][j - 1] + (x.charAt(i - 1) == y.charAt(j - 1) ? 0 : 1);
        ds[i][j] = StringEditMapper.minimum(del, ins, sub);
      }
    }

    return ds[x.length()][y.length()];
  }

  private static int minimum(int a, int b, int c) {
    return Math.min(Math.min(a, b), c);
  }
}

