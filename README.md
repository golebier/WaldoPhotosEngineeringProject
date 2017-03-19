# WaldoPhotosEngineeringProject
#
# Problem Statement
# 
# Using any language and data-store of your choice, write an application that reads a set of photos from a network store (S3), parses the EXIF data from the photos and indexes the EXIF key/value pairs into a query-able store by unique photo.

I used Spark, I use Spark Quite a while and I think to answer: concurrency composition, overall performance and resilience to failures needs it's the best tool. Many things we have for free, yet Spark does not read from http so this part I needed to do on myself.
Code is short and clean.
The method preparing Row for DataFrame is not finished I used my time for problems with Exif extraction.

I'll finish it the next free time, just for fun. But this time I couldn't finish this on, It was fun, new with Exif and I'll finish it.

I used Spark Scala App but to simplify you can execute it from REPL:
spark-shell --master <your master> --jars <path>/sanselan-0.97-incubator.jar,<parh>/commons-imaging-1.0-R1534292.jar -i WaldoPhotosEngineeringProjectApp.scala

# A simple version is finished.
# I have problem with serialization on exception.
# I'll find it next time!

Uncommented lines for one URL shows flow for one URL/JPG
