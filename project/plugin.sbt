addSbtPlugin("org.xerial.sbt"     % "sbt-sonatype"             % "3.9.10")
addSbtPlugin("com.github.sbt"     % "sbt-pgp"                  % "2.1.2")
addSbtPlugin("org.scoverage"      % "sbt-scoverage"            % "1.9.3")
addSbtPlugin("org.scalameta"      % "sbt-scalafmt"             % "2.4.6")
addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.1.0")
addSbtPlugin("com.eed3si9n"       % "sbt-buildinfo"            % "0.10.0")
addSbtPlugin("org.wvlet.airframe" % "sbt-airframe"             % "22.2.0")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")

addDependencyTreePlugin

// For Scala.js
val SCALAJS_VERSION = sys.env.getOrElse("SCALAJS_VERSION", "1.9.0")
addSbtPlugin("org.scala-js"  % "sbt-scalajs"         % SCALAJS_VERSION)
addSbtPlugin("ch.epfl.scala" % "sbt-scalajs-bundler" % "0.20.0")
libraryDependencies ++= (
  Seq("org.scala-js" %% "scalajs-env-jsdom-nodejs" % "1.1.0")
)

// For setting explicit versions for each commit
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.1.1")

// Documentation
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.2.24")

addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.14")

scalacOptions ++= Seq("-deprecation", "-feature")
