package net.imadz.infrastructure.bootstrap

import akka.actor.typed.scaladsl.adapter._
import akka.projection.jdbc.scaladsl.JdbcProjection
import net.imadz.common.application.projection.{ScalikeJdbcSession, ScalikeJdbcSetup}

import javax.inject.{Inject, Singleton}
import scala.concurrent.Await
import scala.concurrent.duration._

/** Establish the JDBC pool and offset tables before any projection starts. */
@Singleton
class FabJdbcBootstrap @Inject()(classicSystem: akka.actor.ActorSystem) {
  private val system = classicSystem.toTyped
  ScalikeJdbcSetup.init(system)
  Await.result(JdbcProjection.createTablesIfNotExists(() => new ScalikeJdbcSession())(system), 30.seconds)
}
