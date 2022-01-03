import cats.effect.{ExitCode, IO, IOApp}
import fs2.{INothing, Pure, Stream, Chunk}

object Fs2Tutorial extends IOApp {

  case class Actor(id: Int, firstName: String, lastName: String)

  // Pure stream (doesn't require any effect)
  val jlActors: Stream[Pure, Actor] = Stream(
    Actor(0, "Henry", "Cavill"),
    Actor(1, "Gal", "Godot"),
    Actor(2, "Ezra", "Miller"),
    Actor(3, "Ben", "Fisher"),
    Actor(4, "Ray", "Hardy"),
    Actor(5, "Jason", "Momoa")
  )

  object ActorRepository {
    def save(actor: Actor): IO[Int] = IO {
      println(s"Saving actor: $actor")
      if (actor.id == 3) throw new RuntimeException("Something went wrong")
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
    Actor(7, "Scarlett", "Johansson"),
    Actor(8, "Robert", "Downey Jr."),
    Actor(9, "Chris", "Evans"),
    Actor(10, "Mark", "Ruffalo"),
    Actor(11, "Chris", "Hemsworth"),
    Actor(12, "Jeremy", "Renner")
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

  val managedJlActors: Stream[IO, AnyVal] = {
    val acquire = IO { println("Acquiring connection to the database") }
    val release = IO { println("Releasing connection to the database") }
    for {
      bracket <- Stream.bracket(acquire)(_ => release)
      saved <- errorHandledSavedJlActors
    } yield ()
  }

  override def run(args: List[String]): IO[ExitCode] = {
    // Compiling evaluates the stream to a single effect, but it doesn't execute it
    managedJlActors.compile.drain.as(ExitCode.Success)
  }
}
