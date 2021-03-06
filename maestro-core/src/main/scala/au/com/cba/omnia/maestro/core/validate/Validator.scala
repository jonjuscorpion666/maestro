//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.core
package validate

import au.com.cba.omnia.maestro.core.data._
import scalaz._, Scalaz._

case class Validator[A](run: A => ValidationNel[String, A])

object Validator {
  def all[A](validators: Validator[A]*): Validator[A] =
    Validator(a => validators.toList.traverseU(_.run(a)).as(a))

  def by[A](validation: A => Boolean, message: String): Validator[A] =
    Validator(a => if (validation(a)) a.success else message.failNel)

  def of[A, B](field: Field[A, B], check: Validator[B]): Validator[A] =
    Validator(a => check.run(field.get(a)).as(a))
}

object Check {
  def oneOf(categories: String*): Validator[String] =
    Validator(s => if (categories.contains(s)) s.success else s"""Expected one of [${categories.mkString("|")}] but got ${s}.""".failNel)

  def nonempty: Validator[String] =
    Validator(s => if (!s.isEmpty) s.success else "Value can not be empty.".failNel)
}
