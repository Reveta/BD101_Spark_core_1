package com.epam.spark

import java.io.{Reader, Writer}

class Parser {

  import java.util

  @throws[Exception]
  def writeLine(w: Writer, values: List[String]): Unit = {
    var firstVal = true
    for (str: String <- values) {
      if (!firstVal) w.write(",")
      w.write("\"")
      var i = 0
      while ( {
        i < str.length
      }) {
        val ch: Char = str.charAt(i)
        if (ch == '\"') w.write("\"") // extra quote
        w.write(ch)

        {
          i += 1
          i - 1
        }
      }
      w.write("\"")
      firstVal = false
    }
    w.write("\n")
  }


  /**
    * returns a row of values as a list
    * returns null if you are past the end of the line
    */
  @throws[Exception]
  def parseLine(r: Reader): util.ArrayList[String] = {
    var ch: Int = r.read
    while ( {
      ch == '\r'
    }) { //ignore linefeed characters wherever they are, particularly just before end of file
      ch = r.read
    }
    if (ch < 0) return null
    val store = new util.ArrayList[String]
    var curVal = new StringBuilder
    var inquotes = false
    var started = false
    while ( {
      ch >= 0
    }) {
      if (inquotes) {
        started = true
        if (ch == '\"') inquotes = false
        else curVal.append(ch.toChar)
      }
      else if (ch == '\"') {
        inquotes = true
        if (started) { // if this is the second quote in a value, add a quote
          // this is for the double quote in the middle of a value
          curVal.append('\"')
        }
      }
      else if (ch == ',') {
        store.add(curVal.toString)
        curVal = new StringBuilder
        started = false
      }
      else if (ch == '\r') {
        //ignore LF characters
      }
      else if (ch == '\n') { //end of a line, break out
      }
      else curVal.append(ch.toChar)
      ch = r.read
    }
    store.add(curVal.toString)
    return store
  }
}
