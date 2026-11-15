import sbt._
import sbt.Keys._
import play.sbt.PlayScala
import play.sbt.routes.RoutesKeys

/** Additive profile for Play only. No runtime source dependency or changes to the legacy build. */
object MonarchR2Profile extends AutoPlugin {
  override def requires: Plugins = PlayScala
  override def trigger: PluginTrigger = allRequirements
  private def enabled: Boolean = sys.props.contains("monarch.r2.version")
  private def durable(file: File): Boolean = file.getName == "FabDurableWorkflowController.scala" ||
    file.getAbsolutePath.replace('\\', '/').contains("/net/imadz/application/durable/")

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    libraryDependencies ++= (if (enabled) Seq("org.iq80.leveldb" % "leveldb" % "0.12") else Seq.empty),
    Test / javaOptions ++= (ThisBuild / dependencyOverrides).value
      .filter(m => m.organization == "net.imadz" && Set("monarch-core", "monarch-persistence")(m.name))
      .map(m => s"-Dtest.${m.name}.version=${m.revision}"),
    Compile / unmanagedSources ~= (files => if (enabled) files else files.filterNot(durable)),
    Test / unmanagedSources ~= (files => if (enabled) files else files.filterNot(durable)),
    Compile / RoutesKeys.routesCompilerTasks ~= { tasks =>
      tasks.filter(t => enabled || t.file.getName != "durablefab.routes").map { task =>
        if (task.file.getName == "durablefab.routes") task.copy(reverseRouter = false) else task
      }
    }
  )
}
