///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

// The following says to import everything except java.io.FileSystem, because
// it conflicts with Hadoop's FileSystem. (Technically, it imports everything
// but in the process aliases FileSystem to _, which has the effect of making
// it inaccessible. _ is special in Scala and has various meanings.)
import java.io.{FileSystem=>_,_}
import java.net.URI

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.ioutil._

package object hadoop {
  class HadoopFileHandler extends FileHandler {
    protected def get_file_system(filename: String) = {
      val conf = new Configuration()
      FileSystem.get(URI.create(filename), conf)
    }

    def get_input_stream(filename: String) =
      get_file_system(filename).open(new Path(filename))
  
    def get_output_stream(filename: String) =
      get_file_system(filename).create(new Path(filename))

    def join_filename(dir: String, file: String) =
      new Path(dir, file).toString

    def is_directory(filename: String) =
      get_file_system(filename).getFileStatus(new Path(filename)).isDir

    def list_files(dir: String) = {
      for (file <- get_file_system(dir).listStatus(new Path(dir)))
        yield file.toString
    }
  }
}
