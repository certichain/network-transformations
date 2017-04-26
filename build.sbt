organization := "net.ilyasergey"

name := "protocol-combinators"

version := "0.5.0"

scalaVersion := "2.11.6"

val akkaVersion = "2.3.9"

libraryDependencies ++= Seq(
        "com.typesafe.akka" %%  "akka-actor"       % akkaVersion,
        "com.typesafe.akka" %%  "akka-slf4j"       % akkaVersion    % "test",
        "ch.qos.logback"    %   "logback-classic"  % "1.1.3"        % "test",
        "com.typesafe.akka" %%  "akka-testkit"     % akkaVersion    % "test",
        "org.scalatest"     %%  "scalatest"        % "2.2.4"        % "test",
        "org.scalaz" %% "scalaz-core" % "7.2.11"
)

scalacOptions ++= Seq(
        "-language:postfixOps",
        "-deprecation",
        "-Xfatal-warnings",
        "-Xlint"
)