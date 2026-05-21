val sparkVersion      = "3.5.3"
val bouncyCastleVer   = "1.80"
val antlr4RuntimeVer  = "4.9.3"
val scalaTestVersion  = "3.2.17"

ThisBuild / scalaVersion            := "2.13.15"
ThisBuild / crossScalaVersions      := Seq("2.13.15", "2.12.20")
ThisBuild / organization            := "io.github.saadaouini"
ThisBuild / organizationName        := "saadaouini"
ThisBuild / dynverSonatypeSnapshots := true  // appends -SNAPSHOT for non-tagged commits

Antlr4 / antlr4Version     := antlr4RuntimeVer
Antlr4 / antlr4GenListener := false
Antlr4 / antlr4GenVisitor  := true
Antlr4 / antlr4PackageName := Some("io.github.sparkasn1.spark.asn1.parser.antlr")

lazy val root = (project in file("."))
  .enablePlugins(Antlr4Plugin)
  .settings(
    name := "spark-asn1",

    // Target Java 11 so sbt-assembly's ASM can shade generated ANTLR4 bytecode.
    // Spark 3.5 itself targets Java 11, so this is the right compatibility floor.
    javacOptions  ++= Seq("--release", "11"),
    scalacOptions ++= Seq("-release", "11"),

    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-sql"      % sparkVersion  % Provided,
      "org.apache.spark"  %% "spark-core"     % sparkVersion  % Provided,
      "org.bouncycastle"  %  "bcprov-jdk18on" % bouncyCastleVer,
      "org.antlr"         %  "antlr4-runtime" % antlr4RuntimeVer,
      "org.scalatest"     %% "scalatest"         % scalaTestVersion  % Test,
      "org.scalatestplus" %% "scalacheck-1-17"  % "3.2.17.0"        % Test,
      "org.scalacheck"    %% "scalacheck"        % "1.17.1"          % Test,
      "org.apache.spark"  %% "spark-sql"         % sparkVersion      % Test,
      "org.apache.spark"  %% "spark-core"        % sparkVersion      % Test,
    ),

    // Shade BouncyCastle and ANTLR4 to avoid classpath conflicts on clusters
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("org.bouncycastle.**" -> "io.github.sparkasn1.shaded.bc.@1").inAll,
      ShadeRule.rename("org.antlr.**"        -> "io.github.sparkasn1.shaded.antlr.@1").inAll,
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case x                                    => (assembly / assemblyMergeStrategy).value(x)
    },
    assembly / assemblyJarName := s"${name.value}-${version.value}-assembly.jar",

    Test / fork              := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      s"-Duser.name=${sys.props.getOrElse("user.name", "spark-test")}",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
      "-Djdk.security.auth.subject.useTL=true"
    ),
    Test / envVars := Map(
      "SPARK_USER"        -> sys.props.getOrElse("user.name", "spark-test"),
      "HADOOP_USER_NAME"  -> sys.props.getOrElse("user.name", "spark-test")
    ),

    publishMavenStyle      := true,
    publishTo              := sonatypePublishToBundle.value,
    sonatypeCredentialHost := "central.sonatype.com",
    usePgpKeyHex("5D4E47CFFB9C4CE1"),

    licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")),
    homepage := Some(url("https://github.com/saadaouini/spark-asn1")),
    description := "Apache Spark data source for ASN.1-encoded files (BER, DER, PER, XER), schema-driven without code generation",
    scmInfo := Some(ScmInfo(
      url("https://github.com/saadaouini/spark-asn1"),
      "scm:git:git://github.com/saadaouini/spark-asn1.git",
      Some("scm:git:ssh://github.com/saadaouini/spark-asn1.git")
    )),
    developers := List(Developer(
      id    = "saadaouini",
      name  = "Nidhal Saadaoui",
      email = "nidhal.saadaoui.1993@gmail.com",
      url   = url("https://github.com/saadaouini")
    )),

    pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray)
  )
