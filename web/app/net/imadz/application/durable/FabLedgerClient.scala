package net.imadz.application.durable

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, IOException, InputStream}
import java.net.{InetAddress, InetSocketAddress, Socket, SocketTimeoutException}
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, ScheduledFuture, ScheduledThreadPoolExecutor, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.control.NonFatal

/** Only a client of the parent's independent durable ledger fixture. No ledger, equipment
  * state, task completion, or retry loop is implemented inside the workflow host.
  * Four workers, sixteen queued calls, bounded frames and a whole-call socket deadline.
  */
final class FabLedgerClient(port: Int) extends AutoCloseable {
  require(port > 0 && port <= 65535, "Fab fixture port must be in 1..65535")
  private val closed = new AtomicBoolean(false)
  private val threadNumber = new AtomicInteger()
  private val threads = new ThreadFactory {
    override def newThread(task: Runnable): Thread = {
      val thread = new Thread(task, s"fab-durable-ledger-${threadNumber.incrementAndGet()}")
      thread.setDaemon(true)
      thread
    }
  }
  private val executor = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](16), threads, new ThreadPoolExecutor.AbortPolicy)
  private val io = ExecutionContext.fromExecutorService(executor)
  private val deadlines = new ScheduledThreadPoolExecutor(1, threads)
  deadlines.setRemoveOnCancelPolicy(true)
  private val sockets = ConcurrentHashMap.newKeySet[Socket]()
  private def closeSocket(socket: Socket): Unit = try socket.close() catch { case NonFatal(_) => () }

  def call(command: String, fields: Vector[String] = Vector.empty): Future[Vector[String]] = {
    val expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(4000L)
    try Future(blocking {
      if (closed.get()) throw new IOException("Fab ledger client is closed")
      val request = FabLedgerWire.frame(command +: fields)
      val remaining = expires - System.nanoTime()
      if (remaining <= 0L) throw new SocketTimeoutException("Fab ledger call expired before dispatch")
      val socket = new Socket()
      sockets.add(socket)
      var alarm: ScheduledFuture[_] = null
      try {
        if (closed.get()) throw new IOException("Fab ledger client is closed")
        alarm = deadlines.schedule(new Runnable {
          override def run(): Unit = closeSocket(socket)
        }, remaining, TimeUnit.NANOSECONDS)
        socket.connect(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port), FabLedgerWire.TimeoutMillis)
        socket.setSoTimeout(FabLedgerWire.TimeoutMillis)
        socket.setTcpNoDelay(true)
        val out = socket.getOutputStream
        out.write(request)
        out.flush()
        FabLedgerWire.read(socket.getInputStream)
      } finally {
        if (alarm != null) alarm.cancel(false)
        sockets.remove(socket)
        closeSocket(socket)
      }
    })(io) catch { case NonFatal(error) => Future.failed(error) }
  }

  override def close(): Unit = if (closed.compareAndSet(false, true)) {
    // Drain queued futures through the closed check rather than abandoning their promises.
    executor.shutdown()
    val active = sockets.iterator()
    while (active.hasNext) closeSocket(active.next())
    deadlines.shutdown()
  }
}

/** Big endian int32 body length, int32 count, then int32 UTF-8 byte length + bytes.
  * Pure framing functions are also used by contract tests, without opening sockets.
  */
private[durable] object FabLedgerWire {
  val MaxWireBytes = 128 * 1024 // Includes the four-byte length prefix.
  val MaxStringBytes = 64 * 1024
  val MaxStrings = 16
  val TimeoutMillis = 2000

  private def ensure(ok: Boolean, reason: String): Unit = if (!ok) throw new IOException(reason)

  def frame(fields: Vector[String]): Array[Byte] = {
    ensure(fields.nonEmpty && fields.size <= MaxStrings, "frame requires 1..16 strings")
    val buffer = new ByteArrayOutputStream()
    val body = new DataOutputStream(buffer)
    body.writeInt(fields.size)
    fields.foreach { field =>
      ensure(field != null && field.length <= MaxStringBytes, "invalid string length")
      val utf8 = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(field))
      val length = utf8.remaining()
      ensure(length <= MaxStringBytes && buffer.size().toLong + 4L + length <= MaxWireBytes - 4L,
        "string or frame exceeds byte limit")
      val bytes = new Array[Byte](length)
      utf8.get(bytes)
      body.writeInt(length)
      body.write(bytes)
    }
    body.flush()
    val framed = new ByteArrayOutputStream()
    val out = new DataOutputStream(framed)
    out.writeInt(buffer.size())
    buffer.writeTo(out)
    out.flush()
    framed.toByteArray
  }

  def read(stream: InputStream): Vector[String] = {
    val wire = new DataInputStream(stream)
    val length = wire.readInt()
    ensure(length >= 4 && length <= MaxWireBytes - 4, "invalid frame byte length")
    val bytes = new Array[Byte](length)
    wire.readFully(bytes)
    val body = new DataInputStream(new ByteArrayInputStream(bytes))
    val count = body.readInt()
    ensure(count >= 1 && count <= MaxStrings, "invalid string count")
    val fields = Vector.newBuilder[String]
    (0 until count).foreach { _ =>
      val size = body.readInt()
      ensure(size >= 0 && size <= MaxStringBytes && size <= body.available(), "invalid UTF-8 byte length")
      val value = new Array[Byte](size)
      body.readFully(value)
      fields += StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(value)).toString
    }
    ensure(body.available() == 0, "trailing bytes in frame body")
    fields.result()
  }
}
