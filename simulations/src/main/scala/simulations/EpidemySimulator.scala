package simulations

import math.random

class EpidemySimulator extends Simulator {

  def randomBelow(i: Int) = (random * i).toInt

  protected[simulations] object SimConfig {
    val population: Int = 300
    val roomRows: Int = 8
    val roomColumns: Int = 8

    // to complete: additional parameters of simulation
    val prevalenceRate = 0.01
    val transmissibilityRate = 0.4
    val timeToBecomeSick: Int = 6
    val timeToDie: Int = 14
    val timeToBecomeImmune: Int = 16
    val timeToHeal: Int = 18
    val probDie: Double = 0.25
    val maxTimeToNextMove: Int = 5
  }

  import SimConfig._

  val persons: List[Person] = (0 until SimConfig.population).map(i => new Person(i)).toList

  util.Random.shuffle(persons)
    .take((SimConfig.prevalenceRate * SimConfig.population).toInt)
    .foreach(_.infected = true)

  persons.foreach{ p =>
    afterDelay(0)(p.move)
    afterDelay(0)(p.changeHealth)
  }

  class Person (val id: Int) {
    var infected = false
    var sick = false
    var immune = false
    var dead = false

    // demonstrates random number generation
    var row: Int = randomBelow(roomRows)
    var col: Int = randomBelow(roomColumns)

    //
    // to complete with simulation logic
    //

    var lastTimeHealthy: Int = 0
    // var timeOfNextMove: Int = randomBelow(maxTimeToNextMove) + 1

    def adjacentRooms = List((row + 1, col), (row - 1, col), (row, col + 1), (row, col - 1))
      .map{ case (r, c) => ((r + 8) % 8, (c + 8) % 8) }

    def isVisiblyInfected: Boolean = sick | dead

    def changeHealth: Unit = {
      if(!dead){
        if(!infected && !sick && !immune){
          infected = persons.exists(p => (row == p.row) && (col == p.col) && p.infected) && (random < SimConfig.transmissibilityRate)
        }

        (currentTime - lastTimeHealthy) match {
          case SimConfig.timeToBecomeSick =>
            infected = true
            sick = true
            immune = false
            dead = false

          case SimConfig.timeToDie =>
            infected = true
            sick = true
            immune = false
            dead = random < SimConfig.probDie

          case SimConfig.timeToBecomeImmune =>
            infected = true
            sick = false
            immune = true
            dead = false

          case SimConfig.timeToHeal =>
            infected = false
            sick = false
            immune = false
            dead = false
            lastTimeHealthy = currentTime
          case _ => ()
        }
      }

      afterDelay(1)(changeHealth)
    }

    def move: Unit = {
      if(!dead){
        val possibleDestinations = for{
          (r, c) <- adjacentRooms
          p <- persons
          if (p.row == r) && (p.col == c) && !p.isVisiblyInfected
        } yield (r, c)

        if(possibleDestinations.nonEmpty){
          val (nextRow, nextCol) = possibleDestinations(randomBelow(possibleDestinations.size))
          row = nextRow
          col = nextCol
        }

        afterDelay(randomBelow(maxTimeToNextMove) + 1)(move)
      }
    }
  }
}
