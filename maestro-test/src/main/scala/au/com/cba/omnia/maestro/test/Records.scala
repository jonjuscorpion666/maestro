package au.com.cba.omnia.maestro.test

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scalaz.effect.IO

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeTools
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.core.ThermometerRecordReader

trait Records {
  object DelimitedRecords {
    def apply[A](conf: Configuration, expectedPath: Path, delimiter: Char, decoder: Decode[A]): List[A] =
      new Context(conf).lines(expectedPath).map(l =>
        decoder.decode(UnknownDecodeSource(l.split(delimiter).toList)) match {
          case DecodeOk(c) => Some(c)
          case _           => None
        }).flatten
  }

  object ParquetThermometerRecordReader {
    def apply[A <: ThriftStruct: Manifest] =
      ThermometerRecordReader((conf, path) => IO {
        ParquetScroogeTools.listFromPath[A](conf, path)
      })
  }
}