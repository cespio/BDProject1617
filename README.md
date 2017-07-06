INTRODUCTION
------------
Authours:
*Alessandro Rizzuto 187156
*Francesco Contaldo 190626
Project:
Data Mining Project "Frequent Pattern Mining on a single Graph"

INSTRUCTIONS
-----------

DOT
---
Both input and output respect the dot notation to describe a graph
https://en.wikipedia.org/wiki/DOT_(graph_description_language)

to transform a dot file into visible pdf format use
'dot -Tps NameFile.dot -o  NameFile.pdf'

Graph Generation
-------------
To generate a random input graph use the following command:
'python graphGen.py NumberOfNodes NumberOfLabel NumberOfHours(weight)'

the second parameter is referred to the number of possible values that can be assigned to a single node. Range of value valid 0..13

the third parameter is used to decide the number of possible values that the different weights (hours) can take. Range of valid value 0..24

The automated generated graph is created in the data folder with the name 'graphGenOut.dot'


Main Program
------------
To mine the graph use:
spark-submit --class "App" --conf 'spark.driver.extraJavaOptions=-Xss1g' --conf 'spark.executor.extraJavaOptions=-Xss1g' JarFileName.jar

Optional --master local[n] to set the number of local nodes

Then will be asked interactively the support threshold and the subgraph maximum size

The found subgraphs are stored in the Result directory with the dot notation.
