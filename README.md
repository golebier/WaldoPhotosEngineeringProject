# WaldoPhotosEngineeringProject
#
# Problem Statement
# 
# Using any language and data-store of your choice, write an application that reads a set of photos from a network store (S3), parses the EXIF data from the photos and indexes the EXIF key/value pairs into a query-able store by unique photo.

I used Spark, I use Spark Quite a while and I think to answer: concurrency composition, overall performance and resilience to failures needs it's the best tool. Many things we have for free, yet Spark does not read from http so this part I needed to do on myself.
Code is short and clean.
The method preparing Row for DataFrame is not finished I used my time for problems with Exif extraction.

I'll finish it on the next free time but this time this one I couldn't finish.
