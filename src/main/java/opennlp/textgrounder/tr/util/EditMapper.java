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
package opennlp.textgrounder.tr.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import opennlp.textgrounder.tr.util.Span;

public class EditMapper<A> {
  protected enum Operation {
    SUB, DEL, INS
  }

  private final int cost;
  private final List<Operation> operations;

  public EditMapper(List<A> s, List<A> t) {
    int[][] ds = new int[s.size() + 1][t.size() + 1];
    Operation[][] os = new Operation[s.size() + 1][t.size() + 1];

    for (int i = 0; i <= s.size(); i++) { ds[i][0] = i; os[i][0] = Operation.DEL; }
    for (int j = 0; j <= t.size(); j++) { ds[0][j] = j; os[0][j] = Operation.INS; }

    for (int i = 1; i <= s.size(); i++) {
      for (int j = 1; j <= t.size(); j++) {
        int del = ds[i - 1][j] + delCost(t.get(j - 1));
        int ins = ds[i][j - 1] + insCost(s.get(i - 1));
        int sub = ds[i - 1][j - 1] + subCost(s.get(i - 1), t.get(j - 1));

        if (sub <= del) {
          if (sub <= ins) {
            ds[i][j] = sub;
            os[i][j] = Operation.SUB;
          } else {
            ds[i][j] = ins;
            os[i][j] = Operation.INS;
          }
        } else {
          if (del <= ins) {
            ds[i][j] = del;
            os[i][j] = Operation.DEL;
          } else {
            ds[i][j] = ins;
            os[i][j] = Operation.INS;
          }
        }
      }
    }

    this.cost = ds[s.size()][t.size()];
    this.operations = new ArrayList<Operation>();

    int i = s.size();
    int j = t.size();

    while (i > 0 || j > 0) {
      this.operations.add(os[i][j]);
      switch (os[i][j]) {
        case SUB: i--; j--; break;
        case INS: j--; break;
        case DEL: i--; break;
      }
    }

    Collections.reverse(this.operations);   
  }

  public int getCost() {
    return this.cost;
  }

  public List<Operation> getOperations() {
    return this.operations;
  }

  protected int delCost(A x) { return 1; }
  protected int insCost(A x) { return 1; }
  protected int subCost(A x, A y) { return x.equals(y) ? -1 : 1; }

  public <B> Span<B> map(Span<B> span) {
    int start = span.getStart();
    int end = span.getEnd();

    int current = 0;
    for (Operation operation : this.operations) {
      switch (operation) {
        case SUB:
          current++;
          break;
        case DEL:
          if (current < span.getStart()) {
            start--;
          } else if (current < span.getEnd()) {
            end--;
          }
          current++;
          break;
        case INS:
          if (current < span.getStart()) {
            start++;
          } else if (current < span.getEnd()) {
            end++;
          }
          break;        
      }
    }

    return new Span<B>(start, end, span.getItem());
  }
}

