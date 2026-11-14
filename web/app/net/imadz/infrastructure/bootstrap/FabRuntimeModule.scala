package net.imadz.infrastructure.bootstrap

import akka.actor.typed.scaladsl.adapter._
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import com.google.inject.{AbstractModule, Provides}

import javax.inject.Singleton

/** Sharding must share the actor system managed by Play. */
class FabRuntimeModule extends AbstractModule {
  override def configure(): Unit = {
    bind(classOf[FabJdbcBootstrap]).asEagerSingleton()
    bind(classOf[ApplicationBootstrap]).asEagerSingleton()
  }

  @Provides
  @Singleton
  // Repositories and FabDemoService (including the controller's UI projections)
  // require sharding, so JDBC readiness is an explicit dependency, not eager order.
  def sharding(system: akka.actor.ActorSystem, jdbcBootstrap: FabJdbcBootstrap): ClusterSharding =
    ClusterSharding(system.toTyped)
}
