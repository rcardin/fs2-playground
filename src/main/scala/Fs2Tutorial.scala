import Fs2Tutorial.Data.*
import Fs2Tutorial.Model.Actor
import Fs2Tutorial.Utils.*
import cats.Id
import cats.effect.std.{Queue, Random}
import cats.effect.{ExitCode, IO, IOApp}
import fs2.{Chunk, INothing, Pipe, Pull, Pure, Stream}

object Fs2Tutorial extends IOApp {

  object Model {
    case class Actor(id: Int, firstName: String, lastName: String)
  }

  object Data {
    val henryCavil: Actor = Actor(0, "Henry", "Cavill")
    val galGodot: Actor = Actor(1, "Gal", "Godot")
    val ezraMiller: Actor = Actor(2, "Ezra", "Miller")
    val benFisher: Actor = Actor(3, "Ben", "Fisher")
    val rayHardy: Actor = Actor(4, "Ray", "Hardy")
    val jasonMomoa: Actor = Actor(5, "Jason", "Momoa")

    val scarlettJohansson: Actor = Actor(7, "Scarlett", "Johansson")
    val robertDowneyJr: Actor = Actor(8, "Robert", "Downey Jr.")
    val chrisEvans: Actor = Actor(9, "Chris", "Evans")
    val markRuffalo: Actor = Actor(10, "Mark", "Ruffalo")
    val chrisHemsworth: Actor = Actor(11, "Chris", "Hemsworth")
    val jeremyRenner: Actor = Actor(12, "Jeremy", "Renner")

    val tomHolland: Actor = Actor(13, "Tom", "Holland")
    val tobeyMaguire: Actor = Actor(14, "Tobey", "Maguire")
    val andrewGarfield: Actor = Actor(15, "Andrew", "Garfield")
  }

  object Utils {
    extension [A] (io: IO[A]) def debug: IO[A] = io.map { value =>
      println(s"[${Thread.currentThread().getName}] $value")
      value
    }

    // A Sink, that is a Pipe with Unit output. Now deprecated in favor of the latter.
    def toConsole[T]: Pipe[IO, T, Unit] = in =>
      in.evalMap(str => IO.println(str))
  }

  // The first thing is that fs2 is a pull based streaming library

  // Streams can be used for two things
  // one is actual streaming IO (take things and transform them without accumulating too much in memory)
  // the other is for control flow
  // (do ten requests, transforms all responses)

  // Pure stream (doesn't require any effect)
  val jlActors: Stream[Pure, Actor] = Stream(
    henryCavil,
    galGodot,
    ezraMiller,
    benFisher,
    rayHardy,
    jasonMomoa
  )

  object ActorRepository {
    def save(actor: Actor): IO[Int] = IO {
      println(s"Saving actor: $actor")
      if (scala.util.Random.nextInt() % 2 == 0) {
        throw new RuntimeException("Something went wrong during the communication with the persistence layer")
      }
      println(s"Saved.")
      actor.id
    }.debug

    def saveWithoutError(actor: Actor): IO[Int] = IO {
      println(s"Saving actor: $actor")
      Thread.sleep(100)
      println(s"Saved.")
      actor.id
    }.debug
  }

  val jlActorList: List[Actor] = jlActors.toList
  val jlActorVector: Vector[Actor] = jlActors.toVector

  val infiniteJlActors: Stream[Pure, Actor] = jlActors.repeat
  val repeatedJLActorsList: List[Actor] = infiniteJlActors.take(12).toList

  // Lifts a stream to an effect
  val liftedJlActors: Stream[IO, Actor] = jlActors.covary[IO]

  val jlActorsEffectfulList: IO[List[Actor]] = liftedJlActors.compile.toList

  // We are no constrained to use the IO effect
  // We can use any effect that implements the following interfaces
  // cats.MonadError[?, Throwable], cats.effect.Sync, cats.effect.Async, cats.effect.Concurrent

  val tomHollandStream: Stream[Pure, Actor] = Stream.emit(tomHolland)
  val spiderMen: Stream[Pure, Actor] = Stream.emits(List(tomHolland, tobeyMaguire, andrewGarfield))

  val savingTomHolland: Stream[IO, Unit] = Stream.eval {
    IO {
      println(s"Saving actor $tomHolland")
      Thread.sleep(1000)
      println("Finished")
    }
  }

  // [error] 95 |  savingTomHolland.toList
  // [error]    |  ^^^^^^^^^^^^^^^^^^^^^^^
  // [error]    |value toList is not a member of fs2.Stream[cats.effect.IO, Unit], but could be made available as an extension method.
  // savingTomHolland.toList

  val savedActor: Stream[IO, Int] = Stream.eval(ActorRepository.save(Actor(6, "Tom", "Hanks")))

  // A Chunk is a strict, finite sequence of values that supports efficient indexed based lookup of elements.
  val avengersActors: Stream[Pure, Actor] = Stream.chunk(Chunk.array(Array(
    scarlettJohansson,
    robertDowneyJr,
    chrisEvans,
    markRuffalo,
    chrisHemsworth,
    jeremyRenner
  )))

  // Fold a stream
  val avengersActorsByFirstName: Stream[Pure, Map[String, List[Actor]]] = avengersActors.fold(Map.empty[String, List[Actor]]) { (map, actor) =>
    map + (actor.firstName -> (actor :: map.getOrElse(actor.firstName, Nil)))
  }

  val avengersActorsFirstNames: Stream[IO, Unit] =
    avengersActors.covary[IO].evalTap(actor => IO(println(actor))).map(_.firstName).through(toConsole)


  // Regardless of how a Stream is built up, each operation takes constant time.
  // So s ++ s2 takes constant time, likewise with s.flatMap(f) and handleErrorWith.
  val dcAndMarvelSuperheroes: Stream[Pure, Actor] = jlActors ++ avengersActors

  val printedJlActors: Stream[IO, Unit] = jlActors.flatMap { actor =>
    Stream.eval(IO.println(actor))
  }

  val evalMappedJlActors: Stream[IO, Unit] = jlActors.evalMap(IO.println)
  val evalTappedJlActors: Stream[IO, Actor] = jlActors.evalTap(IO.println)

  val savedJlActors: Stream[IO, Int] = jlActors.evalMap(ActorRepository.save)

  // Stream evaluation blocks on the first error
  val errorHandledSavedJlActors: Stream[IO, AnyVal] =
    savedJlActors.handleErrorWith(error => Stream.eval(IO.println(s"Error: $error")))

  val attemptedSavedJlActors: Stream[IO, Either[Throwable, Int]] = savedJlActors.attempt
  attemptedSavedJlActors.evalMap {
    case Left(error) => IO.println(s"Error: $error")
    case Right(id) => IO.println(s"Saved actor with id: $id")
  }

  // Acquiring connection to the database
  // Saving actor: Actor(0,Henry,Cavill)
  // Saved.
  // Saving actor: Actor(1,Gal,Godot)
  // Saved.
  // Saving actor: Actor(2,Ezra,Miller)
  // Saved.
  // Saving actor: Actor(3,Ben,Fisher)
  // Error: java.lang.RuntimeException: Something went wrong
  // Releasing connection to the database
  case class DatabaseConnection(connection: String) extends AnyVal
  val managedJlActors: Stream[IO, Int] = {
    val acquire = IO {
      val conn = DatabaseConnection("jlaConnection")
      println(s"Acquiring connection to the database: $conn")
      conn
    }
    val release = (conn: DatabaseConnection) => IO.println(s"Releasing connection to the database: $conn")
    Stream.bracket(acquire)(release).flatMap(conn => savedJlActors)
  }

  // Pipes le us define some stages.
  // A Pipe is a function that takes a stream and returns a stream.
  // A pipe is pretty much a map/flatMap type functional operation but the pipe concept
  // fits nicely into the mental model of a Stream.
  val fromActorToStringPipe: Pipe[IO, Actor, String] = in =>
    in.map(actor => s"${actor.firstName} ${actor.lastName}")

  // ...or fromActorToStringPipe(jlActors)...see the implementation of the through method
  val stringNamesOfJlActors: Stream[IO, Unit] =
    jlActors.through(fromActorToStringPipe).through(toConsole)

  // Pull
  // Nothing means can't return, Unit is "completes with no information"

  val tomHollandActorPull: Pull[Pure, Actor, Unit] = Pull.output1(tomHolland)

  val tomHollandActorStream: Stream[Pure, Actor] = tomHollandActorPull.stream

  val spiderMenActorPull: Pull[Pure, Actor, Unit] = tomHollandActorPull >> Pull.output1(tobeyMaguire) >> Pull.output1(andrewGarfield)

  val avengersActorsPull: Pull[Pure, Actor, Unit] = avengersActors.pull.echo

  val unconsAvengersActors: Pull[Pure, INothing, Option[(Chunk[Actor], Stream[Pure, Actor])]] =  avengersActors.pull.uncons

  val uncons1AvengersActors: Pull[Pure, INothing, Option[(Actor, Stream[Pure, Actor])]] = avengersActors.pull.uncons1

  def takeByName(name: String): Pipe[IO, Actor, Actor] =
    def go(s: Stream[IO, Actor], name: String): Pull[IO, Actor, Unit] =
      s.pull.uncons1.flatMap {
        case Some((hd, tl)) =>
          if (hd.firstName == name) Pull.output1(hd) >> go(tl, name)
          else go(tl, name)
        case None => Pull.done
      }
    in => go(in, name).stream

  val avengersActorsCalledChris: Stream[IO, Unit] =
    avengersActors.through(takeByName("Chris")).through(toConsole)

  val concurrentJlActors: Stream[IO, Actor] = liftedJlActors.evalMap(actor => IO {
    Thread.sleep(400)
    actor
  })

  val liftedAvengersActors: Stream[IO, Actor] = avengersActors.covary[IO]
  val concurrentAvengersActors: Stream[IO, Actor] = liftedAvengersActors.evalMap(actor => IO {
    Thread.sleep(200)
    actor
  })

  val mergedHeroesActors: Stream[IO, Unit] =
    concurrentJlActors.merge(concurrentAvengersActors).through(toConsole)

  val sleepyheadStream: Stream[IO, INothing] = Stream.exec {
    IO {
      Thread.sleep(1000)
      println("Slept for 1s")
    }
  }

  import scala.concurrent.duration._
  val random: IO[Random[IO]] = Random.scalaUtilRandom[IO]
  val queue: IO[Queue[IO, Int]] = Queue.bounded[IO, Int] (10)
  val producer: IO[Unit] = for {
    r <- random
    q <- queue
    p <-
      r.betweenInt(1, 11)
      .flatMap { n =>
        q.offer(n) >> IO.println(s"Produced $n")
      }
      .flatTap(_ => IO.sleep(1.second))
      .foreverM
  } yield IO.println("Producer finished.")

  val consumer: IO[Unit] = for {
    q <- queue
    c <- q.take.flatMap { n =>
      IO.println(s"Consumed $n")
    }.foreverM
  } yield IO.println("Consuming done.")

  val concurrently: Stream[IO, Unit] = Stream.eval(consumer).concurrently(Stream.eval(producer))

  val concurrentHeroesActors: Stream[IO, Unit] =
    concurrentJlActors.concurrently(sleepyheadStream).through(toConsole)

  val eitherHeroesActors: Stream[IO, Unit] =
    concurrentJlActors.either(concurrentAvengersActors).through(toConsole)

  // evalMap is equal to s.flatMap(a => Stream.eval(f(a)))
  // parEvalMap adds the parallelism to the stream
  val parJoinedHeroesActors: Stream[IO, Unit] =
    dcAndMarvelSuperheroes.map(actor => Stream.eval(ActorRepository.save(actor))).parJoin(3).through(toConsole)

//  import cats.effect.unsafe.implicits.global
//
//  savingTomHolland.compile.drain.unsafeRunSync()

  override def run(args: List[String]): IO[ExitCode] = {
    // Compiling evaluates the stream to a single effect, but it doesn't execute it
    // val compiledStream: IO[Unit] = avengersActorsFirstNames.compile.drain
    concurrently.compile.drain.as(ExitCode.Success)
  }

}
