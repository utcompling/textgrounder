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
  public class Span<B> {
    private final int start;
    private final int end;
    private final B item;

    protected Span(int start, int end, B item) {
      this.start = start;
      this.end = end;
      this.item = item;
    }

    public int getStart() {
      return this.start;
    }

    public int getEnd() {
      return this.end;
    }

    public B getItem() {
      return this.item;
    }
  }
