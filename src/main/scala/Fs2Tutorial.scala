import cats.effect.{ExitCode, IO, IOApp}
import fs2.{Chunk, INothing, Pipe, Pull, Pure, Stream}
import Fs2Tutorial.Model.Actor
import Fs2Tutorial.Data.*
import Fs2Tutorial.Utils.*

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
      if (actor.id == 3) throw new RuntimeException("Something went wrong")
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

  // Lifts a stream to an effect
  val liftedJlActors: Stream[IO, Actor] = jlActors.covary[IO]

  // We are no constrained to use the IO effect
  // We can use any effect that implements the following interfaces
  // cats.MonadError[?, Throwable], cats.effect.Sync, cats.effect.Async, cats.effect.Concurrent
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
  val avengersActorsByFirstName: Stream[IO, Unit] = avengersActors.fold(Map.empty[String, List[Actor]]) { (map, actor) =>
    map + (actor.firstName -> (actor :: map.getOrElse(actor.firstName, Nil)))
  }.covary[IO].through(toConsole)

  val avengersActorsFirstNames: Stream[IO, Unit] =
    avengersActors.covary[IO].evalTap(actor => IO(println(actor))).map(_.firstName).through(toConsole)


  // Regardless of how a Stream is built up, each operation takes constant time.
  // So s ++ s2 takes constant time, likewise with s.flatMap(f) and handleErrorWith.
  val dcAndMarvelSuperheroes: Stream[Pure, Actor] = jlActors ++ avengersActors

  val savedJlActors: Stream[IO, Int] = jlActors.evalMap(ActorRepository.save)

  // Stream evaluation blocks on the first error
  val errorHandledSavedJlActors: Stream[IO, AnyVal] =
    savedJlActors.handleErrorWith(error => Stream.eval(IO(println(s"Error: $error"))))

  val attemptedSavedJlActors: Stream[IO, Unit] = savedJlActors.attempt.evalMap {
    case Left(error) => IO(println(s"Error: $error"))
    case Right(id) => IO(println(s"Saved actor with id: $id"))
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
  val managedJlActors: Stream[IO, AnyVal] = {
    val acquire = IO { println("Acquiring connection to the database") }
    val release = IO { println("Releasing connection to the database") }
    Stream.bracket(acquire)(_ => release) >> errorHandledSavedJlActors
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
    println(s"Slept 200ms before pushing $actor to the stream")
    actor
  })

  val mergedHeroesActors: Stream[IO, Unit] =
    concurrentJlActors.merge(concurrentAvengersActors).through(toConsole)

  val concurrentHeroesActors: Stream[IO, Unit] =
    concurrentJlActors.concurrently(concurrentAvengersActors).through(toConsole)

  val eitherHeroesActors: Stream[IO, Unit] =
    concurrentJlActors.either(concurrentAvengersActors).through(toConsole)

  // evalMap is equal to s.flatMap(a => Stream.eval(f(a)))
  // parEvalMap adds the parallelism to the stream
  val parJoinedHeroesActors: Stream[IO, Unit] =
    dcAndMarvelSuperheroes.map(actor => Stream.eval(ActorRepository.save(actor))).parJoin(3).through(toConsole)

  override def run(args: List[String]): IO[ExitCode] = {
    // Compiling evaluates the stream to a single effect, but it doesn't execute it
    avengersActorsFirstNames.compile.drain.as(ExitCode.Success)
  }

}
