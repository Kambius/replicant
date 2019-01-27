name := "replicant"

version := "0.1"

scalaVersion := "2.12.8"

addCommandAlias("validate", ";scalafmtCheck;scalafmtSbtCheck;test")

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-Yno-adapted-args",
  "-Ypartial-unification",
  "-Ywarn-dead-code",
  "-Ywarn-extra-implicit",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused:_",
  "-Xfatal-warnings",
  "-Xfuture",
  "-Xlint:_"
)

val silencerV = "1.2.1"
val logbackV  = "1.2.3"
val akkaV     = "2.5.19"
val circeV    = "0.10.0"

libraryDependencies ++= Seq(
  "io.chrisdavenport"         %% "log4cats-slf4j"     % "0.2.0",
  "ch.qos.logback"            % "logback-core"        % logbackV,
  "ch.qos.logback"            % "logback-classic"     % logbackV,
  "com.github.pureconfig"     %% "pureconfig"         % "0.10.0",
  "com.typesafe.akka"         %% "akka-actor-typed"   % akkaV,
  "com.typesafe.akka"         %% "akka-cluster-typed" % akkaV,
  "com.typesafe.akka"         %% "akka-slf4j"         % akkaV,
  "com.typesafe.akka"         %% "akka-http"          % "10.1.7",
  "org.fusesource.leveldbjni" % "leveldbjni-all"      % "1.8",
  "org.xerial"                % "sqlite-jdbc"         % "3.7.2",
  "org.scalikejdbc"           %% "scalikejdbc"        % "3.3.2",
  "io.circe"                  %% "circe-core"         % circeV,
  "io.circe"                  %% "circe-generic"      % circeV,
  "io.circe"                  %% "circe-parser"       % circeV,
  "de.heikoseeberger"         %% "akka-http-circe"    % "1.23.0",
  "org.typelevel"             %% "cats-core"          % "1.5.0",
  "org.scalatest"             %% "scalatest"          % "3.0.5" % Test,
  "com.github.ghik"           %% "silencer-lib"       % silencerV % Provided
)

libraryDependencies ++= Seq(
  compilerPlugin("org.spire-math"  %% "kind-projector"  % "0.9.8"),
  compilerPlugin("com.github.ghik" %% "silencer-plugin" % silencerV)
)

enablePlugins(JavaAppPackaging)
