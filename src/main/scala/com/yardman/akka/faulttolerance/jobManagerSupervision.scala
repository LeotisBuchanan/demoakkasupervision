package com.knoldus.akka.faulttolerance

import scala.concurrent.duration.DurationInt
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Escalate
import akka.actor.SupervisorStrategy.Restart
import akka.actor.SupervisorStrategy.Resume
import akka.actor.SupervisorStrategy.Stop
import akka.actor.actorRef2Scala
import akka.actor.ActorSystem
import akka.actor.ActorRef
import scala.concurrent.Await
import scala.concurrent.duration._
import _root_.scala.util.Random


//domain model objects
object DomainModel{ 
  case class Job(jobId:String, jobName:String, jobDescription:String)
}

object JobManager {
  case class LoadSetLandedEvent(loadSetId:String)
  case class JobApplicationIdDocument(applicationId:String)
}

class JobManager extends Actor with ActorLogging {
  import DomainModel.Job
  import JobManager._
  import JobExecutioner.RunJobComand
  import JobSelector._
  import JobMonitor._
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: ArithmeticException =>
        println("Resuming the child"); Resume // try Restart here and note the difference in the state
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Escalate
    }

  def receive = {
    case  LoadSetLandedEvent(loadSetId) => {
      println("JobManager here")
      val jobSelector   = context.actorOf(Props[JobSelector], "jobSelector")
      jobSelector ! GetJobCommand(loadSetId)
    
    }

    case job:Job => {
      val jobExecutioner   = context.actorOf(Props[JobExecutioner], "JobExecutioner")
      jobExecutioner !  RunJobComand(job)
      
      
    }

    case JobApplicationIdDocument(applicationId) => {
      //monitor the job
      log.info("monitoring Job")
      val jobMonitor   = context.actorOf(Props[JobMonitor], "JobMonitor")
      jobMonitor ! MonitorJobCommand(applicationId)
    }
    
  }


}


object JobSelector {

  case class  GetJobCommand(loadSetId:String) 

}




class JobSelector extends Actor {
  import JobSelector._
  import DomainModel.Job
  var state = 0

  override def preRestart(reason: Throwable,
    message: Option[Any]) {
    println(s"This is the ugly message that killed me = $message")
  }

  def receive = {
    
    case GetJobCommand(loadsetId) => {
      //for now we just sent back same
      val jobId = Random.nextInt(6000).toString
      val job = new Job(jobId, "job_" + jobId, "spark job")
      println("Job selector here")
      sender ! job
    }
    
  }
}

class SparkJobLauncher{

  import DomainModel.Job

  def launchJob(job:Job):String = {
    println("In sparkLauncher job launched")
    //job will take some random time
    //to launch
    //sometimes it will fail to launch
    //Thread.sleep(Random.nextInt(6))
    //return the application_id for this job
    "application_id"
  }

}


object JobExecutioner {
  import DomainModel.Job
  case class RunJobComand(job:Job)

}


class JobExecutioner extends Actor {

  import JobExecutioner.RunJobComand
  import JobManager. JobApplicationIdDocument

  //this is going to take some time 
  override def preRestart(reason: Throwable,
    message: Option[Any]) {
    
  }

  def receive = {
    
    case RunJobComand(job) => {
      println("in JobExecutioner")
      val sparkJobLauncher = new SparkJobLauncher()
      val applicationId:String = sparkJobLauncher.launchJob(job)
      sender ! JobApplicationIdDocument(applicationId)
     }
  }
}


object JobMonitor {

  case class MonitorJobCommand(applicationId:String)

}

class JobMonitor extends Actor with ActorLogging  {
  import JobMonitor.MonitorJobCommand

  var state = 0

  override def preRestart(reason: Throwable,
                          message: Option[Any]) {
    println(s"This is the ugly message that killed me = $message")
  }

  def receive = {
    case MonitorJobCommand(applicationId) => {
      log.info(s"Monitoring job with application id $applicationId")
      

    }

    case x => {log.info("I have no handler for this message") }
    
  }
}


object JobManagementSupervisionDemo extends App {

  import JobManager._
  
  val system = ActorSystem("JobManagementSystem")
  val jobManager   = system.actorOf(Props[JobManager], "jobManager")
  jobManager! new LoadSetLandedEvent("loadset_01")
  Await.ready(system.whenTerminated, 20 second)
}

