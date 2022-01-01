import cats.effect.{ExitCode, IO, IOApp}
import fs2.{Pure, Stream}

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

  val jlActorList: List[Actor] = jlActors.toList

  override def run(args: List[String]): IO[ExitCode] = ???

}
