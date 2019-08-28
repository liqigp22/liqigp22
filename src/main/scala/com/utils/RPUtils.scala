package com.utils

object RPUtils {
   def request(requsetmode:Int,processnode:Int):List[Double]={
     if(requsetmode == 1 && processnode ==1){
       List[Double](1,0,0)
     }else if (requsetmode ==1 && processnode ==2){
       List[Double](1,1,0)
     }else if  (requsetmode ==1 && processnode ==3){
       List[Double](1,1,1)
     }else {
       List[Double](0,0,0)
     }
   }

  def  Ad(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,adorderid:Int,winPrice:Double,
          adpayment:Double):List[Double] ={
    if (iseffective ==1 && isbilling ==1 && isbid ==1){
      if (iseffective ==1 && isbilling ==1 && iswin == 1 && adorderid != 0){
         List[Double](1,1,winPrice/1000.0,adpayment/1000.0)
      }else{
        List[Double](1,0,0,0)
      }

    }else{
      List[Double](0,0,0,0)
    }
  }

  def click(requestmode:Int,iseffective:Int):List[Double]={
    if(requestmode ==2 && iseffective ==1){
      List[Double](1,0)
    }else if (requestmode == 3  && iseffective ==1){
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }


  }



}


