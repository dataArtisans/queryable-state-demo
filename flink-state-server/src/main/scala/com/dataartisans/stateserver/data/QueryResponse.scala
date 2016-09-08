package com.dataartisans.stateserver.data

//[
//  {
//    "target":"apple",
//    "datapoints":[
//      [10,0]
//    ]
//  }
//]
case class QueryResponse(target: String, datapoints: Array[Array[Long]])

