package net.learningclub.util

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer

import scala.concurrent.{ExecutionContext, Future}

object HttpClient {

  def sendRequest(restUrl: String, httpMethod: String)
                 (implicit system: ActorSystem, materializer: ActorMaterializer, executionContext: ExecutionContext): Future[HttpResponse] = {
    val finalUrl = s"http://localhost:8080/$restUrl"
    httpMethod match {
      case "GET" => Http().singleRequest(HttpRequest(uri = finalUrl, method = HttpMethods.GET))
      case "POST" => Http().singleRequest(HttpRequest(uri = finalUrl, method = HttpMethods.POST))
    }
  }

}
