import cats.effect.{ExitCode, IO, IOApp}
import fs2.{INothing, Pure, Stream, Chunk}

import Fs2Tutorial.Model.Actor
import Fs2Tutorial.Data._

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
    }

    def saveWithoutError(actor: Actor): IO[Int] = IO {
      println(s"Saving actor: $actor")
      Thread.sleep(100)
      println(s"Saved.")
      actor.id
    }
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

  // Regardless of how a Stream is built up, each operation takes constant time.
  // So s ++ s2 takes constant time, likewise with s.flatMap(f) and handleErrorWith.
  val dcAndMarvelSuperheroes: Stream[Pure, Actor] = jlActors ++ avengersActors

  val savedJlActors: Stream[IO, Int] = jlActors.flatMap(actor => Stream.eval(ActorRepository.save(actor)))

  // Stream evaluation blocks on the first error
  val errorHandledSavedJlActors: Stream[IO, AnyVal] =
    savedJlActors.handleErrorWith(error => Stream.eval(IO(println(s"Error: $error"))))

  val attemptedSavedJlActors: Stream[IO, Unit] = savedJlActors.attempt.flatMap {
    case Left(error) => Stream.eval(IO(println(s"Error: $error")))
    case Right(id) => Stream.eval(IO(println(s"Saved actor with id: $id")))
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

  // Pipe? Pull?

  val concurrentJlActors: Stream[IO, Actor] = liftedJlActors.flatMap(actor => Stream.eval(IO {
    Thread.sleep(400)
    actor
  }))

  val liftedAvengersActors: Stream[IO, Actor] = avengersActors.covary[IO]
  val concurrentAvengersActors: Stream[IO, Actor] = liftedAvengersActors.flatMap(actor => Stream.eval(IO {
    Thread.sleep(200)
    println(s"Slept 200ms before pushing $actor to the stream")
    actor
  }))

  val mergedHeroesActors: Stream[IO, Unit] = concurrentJlActors.merge(concurrentAvengersActors).flatMap(actor => Stream.eval(IO(println(actor))))

  val concurrentHeroesActors: Stream[IO, Unit] = concurrentJlActors.concurrently(concurrentAvengersActors).flatMap(actor => Stream.eval(IO(println(actor))))

  val eitherHeroesActors: Stream[IO, Unit] = concurrentJlActors.either(concurrentAvengersActors).flatMap(actor => Stream.eval(IO(println(actor))))

  // TODO Add the thread name to the output
  val parJoinedHeroesActors: Stream[IO, Unit] = dcAndMarvelSuperheroes.map(actor => Stream.eval(ActorRepository.save(actor))).parJoin(2).flatMap(actor => Stream.eval(IO(println(actor))))

  override def run(args: List[String]): IO[ExitCode] = {
    // Compiling evaluates the stream to a single effect, but it doesn't execute it
    parJoinedHeroesActors.compile.drain.as(ExitCode.Success)
  }

}
